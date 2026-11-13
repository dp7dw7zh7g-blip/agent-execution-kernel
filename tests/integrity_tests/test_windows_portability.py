"""Guardia de portabilidad de las PRUEBAS a Windows (g0.8 r33, D-78).

En Linux las pruebas de r32 pasaban; en Windows fallaron por dos defectos DE LAS PRUEBAS (no del código de `src/`) que Linux no puede delatar:

1. `with sqlite3.connect(...) as c:` confirma la transacción pero NO cierra la conexión. Linux deja borrar el archivo abierto; Windows no, y
   `TemporaryDirectory.cleanup` lanzaba `PermissionError: [WinError 32]` (7 errores en r32: 2 en integrity y 5 en unae). Se usa
   `with closing(sqlite3.connect(...)) as c, c:` (cierra y además confirma).
2. Un archivo llamado «con.db»: `CON`, `PRN`, `AUX`, `NUL`, `COM1`-`COM9` y `LPT1`-`LPT9` son nombres de dispositivo reservados en Windows incluso con extensión,
   y SQLite no puede abrirlos («unable to open database file»; `test_journal_size_limit` en r32).

Esta prueba LEE el código de las pruebas (ast) y falla si vuelve alguno de los dos patrones. Límites: solo ve `sqlite3.connect(...)` escrito con ese nombre
completo (no `from sqlite3 import connect`) y solo nombres reservados que sean una cadena literal completa (no los construidos con f-strings). No sustituye a
ejecutar las suites en Windows.
"""
import ast
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TESTS = ROOT / "tests"
RESERVED = re.compile(r"^(?:con|prn|aux|nul|com[1-9]|lpt[1-9])\.[A-Za-z0-9_]+$", re.IGNORECASE)


def is_sqlite_connect(node: ast.AST) -> bool:
    return (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "connect"
            and isinstance(node.func.value, ast.Name) and node.func.value.id == "sqlite3")


def with_without_close(tree: ast.AST) -> list[int]:
    """Líneas de `with sqlite3.connect(...) [as x]:` (el `with` solo confirma; no cierra)."""
    return sorted(n.lineno for n in ast.walk(tree) if isinstance(n, (ast.With, ast.AsyncWith)) and any(is_sqlite_connect(i.context_expr) for i in n.items))


def reserved_names(tree: ast.AST) -> list[tuple[int, str]]:
    """Cadenas literales completas con forma de archivo cuyo nombre es un dispositivo reservado de Windows."""
    return sorted((n.lineno, n.value) for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str) and RESERVED.match(n.value))


def scan(visit):
    found = []
    for path in sorted(TESTS.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        for hit in visit(ast.parse(path.read_text(encoding="utf-8"), filename=str(path))):
            found.append(f"{path.relative_to(ROOT).as_posix()}:{hit}")
    return found


class PruebasPortablesAWindows(unittest.TestCase):
    def test_no_with_sqlite3_connect_without_closing(self):
        self.assertEqual(scan(with_without_close), [], "usar `with closing(sqlite3.connect(...)) as c, c:` (el `with` a secas no cierra la conexión)")

    def test_no_reserved_windows_device_name_as_a_file_name(self):
        self.assertEqual(scan(reserved_names), [], "«con», «prn», «aux», «nul», «com1»-«com9», «lpt1»-«lpt9» no se pueden usar como nombre de archivo en Windows")


class Detectores(unittest.TestCase):
    """Controles negativos: los detectores sí ven el defecto (si no, la guardia no vigilaría nada)."""

    def test_detects_with_without_close_and_accepts_closing(self):
        malo = ast.parse("import sqlite3\nwith sqlite3.connect(p) as c:\n    pass\n")
        bueno = ast.parse("import sqlite3\nfrom contextlib import closing\nwith closing(sqlite3.connect(p)) as c, c:\n    pass\nc = sqlite3.connect(p)\n")
        self.assertEqual(with_without_close(malo), [2])
        self.assertEqual(with_without_close(bueno), [])

    def test_detects_reserved_names_in_any_case_and_accepts_normal_ones(self):
        # solo se cuentan los hallazgos: escribir aquí los nombres reservados como literales haría que la guardia se detectara a sí misma
        self.assertEqual(len(reserved_names(ast.parse('a = "con.db"; b = "NUL.sqlite"; c = "Com1.txt"; d = "lpt9.x"'))), 4)
        self.assertEqual(reserved_names(ast.parse('a = "limite.db"; b = "console.db"; c = "con"; d = "icon.db"; e = "com10.db"')), [])


if __name__ == "__main__":
    unittest.main()
