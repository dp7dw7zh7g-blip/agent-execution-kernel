"""Regresión del runner: comandos o argumentos inválidos deben terminar con código 2 e imprimir los comandos válidos.

Un código 0 ante un error tipográfico daría falsa seguridad (ver DECISIONES_v1_1_1.md, "Robustez del runner de pruebas").
Este test solo invoca rutas de rechazo: no ejecuta ninguna suite pesada. (No se prueba `--skip-crash` sin comando:
el runner lo interpreta como `all` por diseño, y eso ejecutaría todo el proyecto.)
"""
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "run_tests.py"


def _run(*args):
    return subprocess.run([sys.executable, str(RUNNER), *args], capture_output=True, text=True, cwd=ROOT, timeout=60)


class RunnerRejectsInvalidInput(unittest.TestCase):
    def assertRejected(self, *args):
        r = _run(*args)
        self.assertEqual(r.returncode, 2, f"{args}: rc={r.returncode}, se esperaba 2 (stderr={r.stderr!r})")
        self.assertIn("Comandos válidos", r.stderr, f"{args}: no imprime los comandos válidos")
        for cmd in ("adapter-integration", "integrity", "patches"):
            self.assertIn(cmd, r.stderr, f"{args}: falta '{cmd}' en la lista de comandos válidos")

    def test_typo_in_command_is_rejected(self):
        self.assertRejected("adaptr")

    def test_unknown_flag_is_rejected(self):
        self.assertRejected("adapter", "--bogus")

    def test_extra_positional_argument_is_rejected(self):
        self.assertRejected("unae", "extra")


if __name__ == "__main__":
    unittest.main()
