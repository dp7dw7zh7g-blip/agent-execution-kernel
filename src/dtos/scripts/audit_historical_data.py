"""Auditoría de SOLO LECTURA de la evidencia persistida en una base DTOS.

No modifica nada: abre la base con `mode=ro` y `query_only`, no ejecuta ningún UPDATE/DELETE/INSERT y no depende de los triggers append-only. El archivo .db no cambia ni un byte; si la base está en modo WAL, SQLite puede crear junto a ella los auxiliares vacíos `-wal`/`-shm` (y necesita poder escribir en el directorio). Informa; la decisión
de qué hacer con una fila infractora (en tablas append-only no hay forma legítima de corregirla en sitio) es del operador.

    PYTHONPATH=src python -m dtos.scripts.audit_historical_data RUTA.db [--json]

Qué comprueba, por tabla y columna:
- `effects.evidence` y `effect_reconciliations.evidence`: JSON válido, sin NaN/Infinity/-Infinity, forma CANÓNICA (`canonical_json(parse(texto)) == texto`)
  y tamaño <= `MAX_EVIDENCE_SIZE_BYTES` (bytes UTF-8). Incumplir cualquiera es VIOLACIÓN. `effects.evidence` NULL es válido (filas previas al parche 0003).
- `recovery_triggers.evidence` y `evidence.contradictions`: JSON válido y sin valores no finitos (VIOLACIÓN). Forma canónica y tamaño solo se
  informan como AVISO: el tope de 65536 bytes y la forma canónica se definieron para la evidencia de efectos, no para estas columnas.

Código de salida: 0 sin violaciones (puede haber avisos), 1 con violaciones, 2 si no se pudo abrir/leer la base.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from contextlib import closing
from pathlib import Path
from typing import Any

from dtos.canonical import canonical_json
from dtos.store import MAX_EVIDENCE_SIZE_BYTES

# (tabla, clave primaria, columna, anulable, el tope y la forma canónica son OBLIGATORIOS)
TARGETS = (
    ("effects", "effect_id", "evidence", True, True),
    ("effect_reconciliations", "reconciliation_id", "evidence", False, True),
    ("recovery_triggers", "trigger_id", "evidence", False, False),
    ("evidence", "evidence_id", "contradictions", False, False),
)


class _NonFinite(ValueError):
    pass


def _reject_constant(name: str) -> Any:
    raise _NonFinite(name)


def inspect_text(text: str, strict: bool) -> list[tuple[str, str]]:
    """Devuelve [(gravedad, motivo)] para un texto JSON; vacío si cumple. gravedad: VIOLACION | AVISO."""
    found: list[tuple[str, str]] = []
    soft = "VIOLACION" if strict else "AVISO"
    size = len(text.encode("utf-8"))
    if size > MAX_EVIDENCE_SIZE_BYTES:
        found.append((soft, f"tamaño {size} B > {MAX_EVIDENCE_SIZE_BYTES} B"))
    try:
        obj = json.loads(text, parse_constant=_reject_constant)
    except _NonFinite as exc:
        found.append(("VIOLACION", f"valor no finito ({exc})"))
        return found
    except ValueError as exc:
        found.append(("VIOLACION", f"JSON inválido ({exc})"))
        return found
    try:
        if canonical_json(obj) != text:
            found.append((soft, "no está en forma canónica"))
    except (ValueError, TypeError) as exc:  # p. ej. float desbordado (1e999 se lee como inf)
        found.append(("VIOLACION", f"no serializable de forma canónica ({exc})"))
    return found


def audit(db_path: str | Path) -> dict[str, Any]:
    path = Path(db_path)
    if not path.is_file():
        raise FileNotFoundError(path)
    report: dict[str, Any] = {"database": str(path), "max_evidence_size_bytes": MAX_EVIDENCE_SIZE_BYTES, "tables": {}, "findings": []}
    with closing(sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)) as conn:
        conn.execute("PRAGMA query_only=ON")
        for table, pk, column, nullable, strict in TARGETS:
            cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
            if column not in cols:
                report["tables"][f"{table}.{column}"] = {"estado": "ausente (base anterior a la columna o a la tabla)", "filas": 0}
                continue
            rows = scanned = nulls = 0
            for key, value in conn.execute(f"SELECT {pk}, {column} FROM {table} ORDER BY {pk}"):
                rows += 1
                if value is None:
                    nulls += 1
                    if not nullable:
                        report["findings"].append({"tabla": table, "id": key, "columna": column, "gravedad": "VIOLACION", "motivo": "NULL en columna NOT NULL"})
                    continue
                scanned += 1
                if not isinstance(value, str):
                    report["findings"].append({"tabla": table, "id": key, "columna": column, "gravedad": "VIOLACION", "motivo": f"no es texto ({type(value).__name__})"})
                    continue
                for gravedad, motivo in inspect_text(value, strict):
                    report["findings"].append({"tabla": table, "id": key, "columna": column, "gravedad": gravedad, "motivo": motivo})
            report["tables"][f"{table}.{column}"] = {"filas": rows, "revisadas": scanned, "nulas": nulls}
    report["violaciones"] = sum(f["gravedad"] == "VIOLACION" for f in report["findings"])
    report["avisos"] = sum(f["gravedad"] == "AVISO" for f in report["findings"])
    return report


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Auditoría de solo lectura de la evidencia persistida en una base DTOS.")
    ap.add_argument("database")
    ap.add_argument("--json", action="store_true", help="salida en JSON")
    args = ap.parse_args(argv)
    try:
        rep = audit(args.database)
    except (OSError, sqlite3.Error) as exc:
        print(f"ERROR: no se pudo auditar {args.database}: {exc}", file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(rep, ensure_ascii=False, indent=2))
    else:
        print(f"Base: {rep['database']}  (tope de evidencia: {rep['max_evidence_size_bytes']} B)")
        for name, info in rep["tables"].items():
            print(f"  {name}: {info}")
        for f in rep["findings"]:
            print(f"  [{f['gravedad']}] {f['tabla']}.{f['columna']} id={f['id']}: {f['motivo']}")
        print(f"Violaciones: {rep['violaciones']}  Avisos: {rep['avisos']}  (solo lectura: la base no se modificó)")
    return 1 if rep["violaciones"] else 0


if __name__ == "__main__":
    sys.exit(main())
