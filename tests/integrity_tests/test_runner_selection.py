"""Regresión (g0.8 r23): `run_tests.py unae --skip-crash` omite SOLO `test_crash`.

Hasta r22 usaba el patrón `test_[!c]*.py`, que también dejaba fuera `test_concurrency.py` (y cualquier archivo que empezara por «c»): el
resultado «OK» de `--skip-crash` cubría menos pruebas de las que parecía. El descubrimiento se mide en un subproceso para no mezclar los
módulos homónimos de `unae_tests` y `dtos_tests` (`test_store`, `test_concurrency`) en este intérprete.
"""
import json
import os
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def collected(directory, skip_crash):
    code = ("import json, run_tests\n"
            f"s = run_tests.collect_unittest({str(ROOT / 'tests' / directory)!r}, skip_crash={skip_crash!r})\n"
            "print(json.dumps([t.id() for t in s]))")
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, cwd=ROOT, timeout=120,
                       env={**os.environ, "PYTHONPATH": str(ROOT / "src")})
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout.strip().splitlines()[-1])


def modules(ids):
    return {i.split(".")[0] for i in ids}


class SkipCrashSelection(unittest.TestCase):
    def test_skip_crash_drops_only_test_crash(self):
        full, skipped = collected("unae_tests", False), collected("unae_tests", True)
        self.assertIn("test_crash", modules(full))
        self.assertNotIn("test_crash", modules(skipped))
        self.assertEqual(modules(full) - modules(skipped), {"test_crash"})

    def test_skip_crash_keeps_test_concurrency(self):
        """El defecto de r22: el patrón `[!c]` también quitaba este módulo."""
        self.assertIn("test_concurrency", modules(collected("unae_tests", True)))

    def test_skip_crash_removes_nothing_else(self):
        full, skipped = collected("unae_tests", False), collected("unae_tests", True)
        self.assertEqual(sorted(i for i in full if i.split(".")[0] != "test_crash"), sorted(skipped))

    def test_without_skip_crash_everything_is_collected(self):
        self.assertGreaterEqual(len(collected("unae_tests", False)), len(collected("unae_tests", True)) + 3)


if __name__ == "__main__":
    unittest.main()
