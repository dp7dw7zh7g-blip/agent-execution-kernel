"""Integridad del release v1.1.1 (Parte 4, Fase 1): ningún archivo de código alterado, añadido o ausente respecto al manifiesto.

Alcance del manifiesto = el del comando que lo genera (ver meta/MANIFEST.md): `src`, `config`, `schemas` y `tests`, sin `__pycache__`.
Si esta prueba falla tras un cambio LEGÍTIMO, regenere el manifiesto (comando en meta/MANIFEST.md); si falla sin cambio deliberado, el árbol se alteró.
"""
import hashlib
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
META = ROOT / "meta"
SCOPE = ("src", "config", "schemas", "tests")
MANIFEST = META / "SHA256SUMS_v1_1_1.txt"
CURRENT = META / "SHA256SUMS.txt"
BASELINE = META / "SHA256SUMS_contrato_v1_1_original.txt"
# Línea base contractual v1.1: inmutable (Fase 1, paso 1). Si este hash cambia, alguien tocó el archivo de auditoría.
BASELINE_SHA256 = "c4b0decd68d72cb42046b7b4e561feeada6e4dfbc871bbf836d28b2dfe5d775e"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        digest, sep, rel = line.partition("  ")
        if not sep or len(digest) != 64 or not rel:
            raise AssertionError(f"{path.name}:{n}: línea mal formada")
        if rel in out:
            raise AssertionError(f"{path.name}:{n}: ruta repetida {rel}")
        out[rel] = digest
    return out


def files_in_scope() -> set[str]:
    found = set()
    for top in SCOPE:
        for p in (ROOT / top).rglob("*"):
            if p.is_file() and "__pycache__" not in p.parts and p.suffix != ".pyc":
                found.add(p.relative_to(ROOT).as_posix())
    return found


class ReleaseIntegrity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.manifest = parse(MANIFEST)

    def test_every_listed_file_matches_its_hash(self):
        bad = [rel for rel, h in self.manifest.items() if not (ROOT / rel).is_file() or sha256(ROOT / rel) != h]
        self.assertEqual(bad, [], f"archivos alterados o ausentes respecto a {MANIFEST.name}")

    def test_no_file_outside_the_release(self):
        extra = sorted(files_in_scope() - set(self.manifest))
        self.assertEqual(extra, [], f"archivos en {SCOPE} que no figuran en {MANIFEST.name}")

    def test_manifest_has_only_files_in_scope(self):
        outside = sorted(r for r in self.manifest if r.split("/", 1)[0] not in SCOPE or "__pycache__" in r)
        self.assertEqual(outside, [])

    def test_current_manifest_is_a_copy_of_the_release_manifest(self):
        self.assertEqual(CURRENT.read_bytes(), MANIFEST.read_bytes())


class AuditBaseline(unittest.TestCase):
    def test_contract_baseline_is_untouched(self):
        self.assertEqual(sha256(BASELINE), BASELINE_SHA256)

    def test_baseline_entries_still_traceable(self):
        """Cada archivo de la línea base v1.1 está en el árbol con EXACTAMENTE el hash original (sin excepciones)."""
        where = {
            "Contrato_Interoperabilidad_UNAE_DTOS_v1.1.docx": "docs/contratos/Contrato_Interoperabilidad_UNAE_DTOS_v1_1.docx",
            "Contrato_Interoperabilidad_UNAE_DTOS_v1.1.md": "docs/contratos/Contrato_Interoperabilidad_UNAE_DTOS_v1_1.md",
            "DECISIONES_v1.1.md": "docs/decisiones/DECISIONES_v1_1.md",
            "unae_dtos_message.schema.json": "docs/contratos/unae_dtos_message.schema.v1_1_original.json",
        }
        entries = [line.partition("  ")[::2] for line in BASELINE.read_text(encoding="utf-8").splitlines()]
        self.assertEqual(sorted(n for _, n in entries), sorted(where), "la línea base tiene entradas sin ubicación conocida")
        for digest, name in entries:
            self.assertEqual(sha256(ROOT / where[name]), digest, name)


if __name__ == "__main__":
    unittest.main()
