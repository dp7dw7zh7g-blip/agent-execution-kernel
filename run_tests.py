#!/usr/bin/env python3
"""Ejecuta las suites por separado (AT-16: unitarias y de integración se reportan aparte).

  python run_tests.py unae      # unittest sobre tests/unae_tests
  python run_tests.py dtos      # scripts con main() sobre tests/dtos_tests (no son unittest)
  python run_tests.py proactive # capa proactiva (modo «solo registrar», base SQLite propia): tests/proactive_tests
  python run_tests.py adapter   # unitarias del adaptador: tests/adapter_tests
  python run_tests.py adapter-integration   # integración (adaptador + Stores reales): tests/adapter_integration_tests
  python run_tests.py integrity # integridad del release (manifiesto SHA-256) y gobernanza de esquemas: tests/integrity_tests
  python run_tests.py patches   # el código == base v1.0/v1.1 + parches 0001…0010 y 0012…0024 en orden (requiere `patch`): tests/patch_tests
  python run_tests.py all       # las suites se ejecutan y se reportan por separado (AT-16)
  --strict (cualquier comando): falla si hay errores, fallos u omisiones no permitidas (p. ej. SchemaConformance sin jsonschema)
  --fast (cualquier comando): carga reducida en las pruebas de WAL (variable AEK_TESTS_FAST=1; D-79). Mismas comprobaciones con menos filas: ~60 % menos
         bytes y ~90 % menos fsync. Sirve para el ciclo diario; la corrida de release (sin --fast) sigue siendo la completa.
Opciones para unae: --skip-crash omite test_crash (el barrido SIGKILL es la parte lenta).
"""
import subprocess, sys, os, unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
ENV = {**os.environ, "PYTHONPATH": str(SRC)}


# Omisiones legítimas por diseño, por suite. Cualquier otra omisión falla en modo --strict.
# unae: ninguna omisión legítima salvo test_crash sin SIGKILL (D-80: test_fsm_parity_with_dtos ya no existe; el contrato declara las FSM separadas).
import signal

# test_crash se omite SOLO si el sistema no tiene SIGKILL (Windows); con SIGKILL esas omisiones siguen siendo un fallo bajo --strict.
CRASH_TESTS_WITHOUT_SIGKILL = frozenset() if hasattr(signal, "SIGKILL") else frozenset(
    {"test_clean_run_reaches_expected_final_state", "test_sigkill_at_every_injection_point", "test_sigkill_during_schema_migration_is_atomic"})
ALLOWED_SKIPS = {"unae": CRASH_TESTS_WITHOUT_SIGKILL}


CRASH_MODULE = "test_crash"   # el ÚNICO módulo que --skip-crash omite, por nombre exacto


def collect_unittest(start, *, skip_crash=False) -> unittest.TestSuite:
    """Descubre los tests de `start` (patrón `test_*.py`). Con `skip_crash` quita SOLO los del módulo `test_crash`.
    Antes (hasta g0.8 r22) --skip-crash usaba el patrón `test_[!c]*.py`, que además dejaba fuera `test_concurrency.py` y cualquier archivo
    de prueba cuyo nombre empezara por «c»: en r22 `unae --skip-crash` ejecutaba 90 pruebas en vez de 92 (las 2 de test_concurrency); el total con test_crash es 95."""
    def flat(suite):
        for t in suite:
            if isinstance(t, unittest.TestSuite):
                yield from flat(t)
            else:
                yield t
    discovered = unittest.TestLoader().discover(str(start), pattern="test_*.py")
    # Un fallo de importación llega como `unittest.loader._FailedTest` (módulo `unittest.loader`): nunca se filtra.
    return unittest.TestSuite(t for t in flat(discovered)
                              if not (skip_crash and type(t).__module__.rsplit(".", 1)[-1] == CRASH_MODULE))


def _run_suite(start, skip_crash, strict, allow) -> int:
    """Ejecuta la suite en ESTE proceso (el runner la lanza en uno aparte). En modo estricto falla además por omisiones no permitidas."""
    import importlib.util  # test_envelope lo usaba sin importarlo (defecto latente)
    result = unittest.TextTestRunner(verbosity=2).run(collect_unittest(start, skip_crash=skip_crash))
    bad = [t.id() for t, _ in result.skipped if t.id().rsplit(".", 1)[-1] not in set(allow)] if strict else []
    if bad:
        print("STRICT: omisiones no permitidas:", bad, file=sys.stderr)
    return 0 if result.wasSuccessful() and not bad else 1


def unittest_suite(name, start, skip_crash=False, strict=False, allow=frozenset()):
    print(f"\n=== {name}{' [strict]' if strict else ''} ===", flush=True)
    code = f"import sys, run_tests; sys.exit(run_tests._run_suite({str(start)!r}, {bool(skip_crash)!r}, {bool(strict)!r}, {sorted(allow)!r}))"
    return subprocess.call([sys.executable, "-c", code], cwd=ROOT, env=ENV)


def dtos_suite():
    d = ROOT / "tests" / "dtos_tests"
    rc = 0
    for f in sorted(d.glob("test_*.py")):
        print(f"\n=== dtos: {f.name} ===", flush=True)
        code = f"import sys; sys.path[:0]=[{str(SRC)!r},{str(d)!r}]; import {f.stem} as m; sys.exit(m.main())"
        r = subprocess.call([sys.executable, "-c", code], cwd=ROOT, env=ENV)
        rc |= r
    return rc


COMANDOS = ("unae", "dtos", "proactive", "adapter", "adapter-integration", "integrity", "patches", "all")


OPCIONES = ("--skip-crash", "--strict", "--fast")


def _uso() -> str:
    return ("Comandos válidos: " + ", ".join(COMANDOS)
            + "\nOpciones: --skip-crash (solo con unae); --strict (falla si hay omisiones no permitidas); --fast (carga reducida en las pruebas de WAL)")


def _chequeo_python():
    """Rechaza intérpretes < 3.11 (StrEnum) con código 2, antes de procesar ninguna suite."""
    if sys.version_info < (3, 11):
        print(f"error: se requiere Python >= 3.11 (intérprete actual: {sys.version.split()[0]}, {sys.executable})", file=sys.stderr)
        sys.exit(2)
    try:
        import jsonschema
        estado = f"jsonschema {jsonschema.__version__}"
    except ImportError:
        estado = "jsonschema NO instalado (SchemaConformance se omitirá; use --strict para fallar)"
    print(f"python {sys.version.split()[0]} | {estado}", flush=True)


if __name__ == "__main__":
    _chequeo_python()
    # Fail closed: un comando desconocido NO puede terminar con código 0 (un error tipográfico daría falsa seguridad).
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    flags = [a for a in sys.argv[1:] if a.startswith("--")]
    if len(args) > 1 or any(f not in OPCIONES for f in flags) or (args and args[0] not in COMANDOS):
        print(f"error: argumentos no válidos: {sys.argv[1:]}\n{_uso()}", file=sys.stderr)
        sys.exit(2)
    which = args[0] if args else "all"
    if "--fast" in flags:
        ENV["AEK_TESTS_FAST"] = "1"   # lo heredan los subprocesos de cada suite (ENV se pasa a subprocess.call)
        print("MODO RÁPIDO (--fast): las pruebas de WAL usan carga reducida; NO sustituye a la corrida completa del release.", flush=True)
    skip = "--skip-crash" in flags
    strict = "--strict" in flags
    rc = 0
    if which in ("unae", "all"):
        rc |= unittest_suite("unae", ROOT / "tests" / "unae_tests", skip, strict, ALLOWED_SKIPS["unae"])
    if which in ("dtos", "all"):
        rc |= dtos_suite()
    if which in ("proactive", "all"):
        rc |= unittest_suite("proactive (capa proactiva)", ROOT / "tests" / "proactive_tests", strict=strict)
    if which in ("adapter", "all"):
        rc |= unittest_suite("adapter (unitarias)", ROOT / "tests" / "adapter_tests", strict=strict)
    if which in ("adapter-integration", "all"):
        rc |= unittest_suite("adapter (integración)", ROOT / "tests" / "adapter_integration_tests", strict=strict)
    if which in ("integrity", "all"):
        rc |= unittest_suite("integridad del release", ROOT / "tests" / "integrity_tests", strict=strict)
    if which in ("patches", "all"):
        rc |= unittest_suite("serie de parches", ROOT / "tests" / "patch_tests", strict=strict)
    sys.exit(rc)
