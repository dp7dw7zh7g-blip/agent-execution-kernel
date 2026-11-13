"""Trazabilidad de parches (Parte 4, Fase 2): el código fuente ES la base v1.0/v1.1 + los parches 0001…0010, 0012, 0013, 0014, 0015, 0016, 0017, 0018, 0019, 0020, 0021, 0022, 0023 y 0024 (el 0011 está reservado a la revalidación de mandatos), aplicados en orden.

Todo ocurre en un directorio temporal; el árbol real no se toca. Requiere el programa `patch` (GNU patch); sin él, las pruebas se OMITEN
(y `run_tests.py patches` lo avisa): en ese caso la comprobación no se hizo.
"""
import hashlib
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PATCHES = ROOT / "patches"
SERIES = [
    "0001-dtos-reconcile-atomico-y-lectura-publica-del-ledger.patch",
    "0002-dtos-canonical-json-rechaza-nan-e-infinity.patch",
    "0003-dtos-evidencia-en-el-ledger.patch",
    "0004-unae-get-operation.patch",
    "0005-dtos-historial-de-reconciliaciones.patch",
    "0006-dtos-tope-de-tamano-de-la-evidencia.patch",
    "0007-unae-pausa-logica-en-dispatch-outbox.patch",
    "0008-dtos-contenido-de-politicas.patch",
    "0009-dtos-reg017-mandatos-y-politicas-inmutables.patch",
    "0010-dtos-concurrencia-busy-timeout-y-wal.patch",
    "0012-dtos-lecturas-publicas-del-ledger-la-auditoria-y-el-outbox.patch",  # el 0011 queda reservado (revalidación de mandatos)
    "0013-dtos-endurecimiento-de-outbox-y-auditoria.patch",
    "0014-dtos-unae-cuarentena-lease-portable-y-endurecimiento.patch",
    "0015-dtos-unae-requeue-cuarentena-y-resiliencia-crash.patch",
    "0016-dtos-reintento-acotado-de-la-apertura.patch",
    "0017-dtos-unae-rollback-seguro-y-claves-canonicas-str.patch",
    "0018-dtos-unae-indice-parcial-del-outbox-purga-y-wal-verificado.patch",
    "0019-dtos-verificacion-de-la-cadena-de-auditoria-por-lotes.patch",
    "0020-dtos-backup-en-caliente-con-vacuum-into.patch",
    "0021-dtos-journal-size-limit.patch",
    "0022-unae-busy-timeout-y-journal-size-limit-parametrizables.patch",
    "0023-dtos-unae-tope-de-busy-timeout-y-normalizacion-a-int-exacto.patch",
    "0024-unae-reintento-acotado-de-la-apertura.patch",
]
# Archivos que la serie modifica, con el SHA-256 completo de la BASE (originales aportados por el usuario).
BASE_SHA256 = {
    "src/dtos/schema.sql": "9b88a721264ebed14c5a8a83c18f2bea0b05780fa6ca79eb9c0e937e65f3b264",
    "src/dtos/store.py": "cc355b3405e182a8640113efdbaf24185d749d88244cde1b7d50f6323c842838",
    "src/dtos/effect_repository.py": "8016df2bdf47093ab06f1ab5d48600e4eae2abb4c8e6699dd01ebf045e5f1f7a",
    "src/dtos/canonical.py": "7cf9f2344353bf20471dda6a2a0e1ce8a8e19a93408e1be1d1f00ccb249cd863",
    "src/dtos/enums.py": "a7e9ffdba9c019fef2181b297c6eee5909f20d0f0468c8fb83d2a8bb1edacd13",  # base = el de g0.6 r14, que el MANIFEST declara copia del original (aún sin parchar)
    "src/dtos/registries.py": "9fb44dd408ed92a83e637a6a592c3182f45ef35ec2752d39048b8908a816fae4",  # base = el de g0.6 r14, que el MANIFEST declara copia del original (aún sin parchar)
    "src/unae/store.py": "3af567b53a88f11ee5e75e9d92727ca699e39a4ccea4073e508dc603a86b2e38",
    "src/dtos/audit.py": "d947795704fca0b1f0380130b3d3d0707d93f21ee317ee2c9375bbdb9e8a5ef1",  # base = el de g0.8 r12, que el MANIFEST declara copia del original (sin hash independiente); lo tocan solo el 0012, el 0013 y el 0014
    "src/dtos/inbox_outbox.py": "7ccafebd17c650ff9d5e391ac0a5d00eb89138046762e8bff1ace5f6af2c5268",  # ídem
}
HAVE_PATCH = shutil.which("patch") is not None


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_patch(workdir: Path, name: str, *, reverse=False, dry=False) -> subprocess.CompletedProcess:
    cmd = ["patch", "-p1", "-F0", "--no-backup-if-mismatch", "-s", "-d", str(workdir), "-i", str(PATCHES / name)]
    if reverse:
        cmd.append("-R")
    if dry:
        cmd.append("--dry-run")
    return subprocess.run(cmd, capture_output=True, text=True)


def touched(name: str) -> list[str]:
    return re.findall(r"^\+\+\+ b/(\S+)", (PATCHES / name).read_text(encoding="utf-8"), re.M)


def tree_with(files) -> Path:
    tmp = Path(tempfile.mkdtemp(prefix="patch-series-"))
    for rel in files:
        dst = tmp / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / rel, dst)
    return tmp


@unittest.skipUnless(HAVE_PATCH, "no hay `patch` en el PATH: la serie NO se verificó")
class PatchSeries(unittest.TestCase):
    def setUp(self):
        self.tmp = tree_with(BASE_SHA256)
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

    def reconstruct_base(self):
        for name in reversed(SERIES):
            r = run_patch(self.tmp, name, reverse=True)
            self.assertEqual(r.returncode, 0, f"no se pudo revertir {name}: {r.stdout}{r.stderr}")

    def test_series_touches_exactly_the_documented_files(self):
        self.assertEqual({f for n in SERIES for f in touched(n)}, set(BASE_SHA256))
        self.assertEqual(sorted(p.name for p in PATCHES.glob("*.patch")), SERIES, "hay parches fuera de la serie o falta alguno")

    def test_reverting_the_series_reproduces_the_original_files(self):
        self.reconstruct_base()
        for rel, digest in BASE_SHA256.items():
            self.assertEqual(sha256(self.tmp / rel), digest, f"{rel}: la base reconstruida no coincide con el original")

    def test_applying_the_series_in_order_reproduces_the_tree(self):
        self.reconstruct_base()
        for name in SERIES:
            r = run_patch(self.tmp, name)
            self.assertEqual(r.returncode, 0, f"{name} no aplica sobre la base+anteriores: {r.stdout}{r.stderr}")
        for rel in BASE_SHA256:
            self.assertEqual((self.tmp / rel).read_bytes(), (ROOT / rel).read_bytes(), f"{rel} difiere de base + parches")

    def test_out_of_order_application_is_rejected(self):
        """Los parches son interdependientes: el 0024 (o el 0023, el 0022, el 0021, el 0020, el 0019, el 0018, el 0017, el 0016, el 0015, el 0014, 0013, 0012, 0010, 0009, 0008, 0006, 0005, 0003) sobre la base sin los anteriores NO debe aplicar."""
        self.reconstruct_base()
        for name in (SERIES[22], SERIES[21], SERIES[20], SERIES[19], SERIES[18], SERIES[17], SERIES[16], SERIES[15], SERIES[14], SERIES[13], SERIES[12], SERIES[11], SERIES[10], SERIES[9], SERIES[8], SERIES[7], SERIES[5], SERIES[4], SERIES[2]):
            self.assertNotEqual(run_patch(self.tmp, name, dry=True).returncode, 0, f"{name} aplicó fuera de orden")


class PatchIndex(unittest.TestCase):
    """patches/README.md debe declarar el SHA-256 real de cada parche (no depende de `patch`)."""

    def test_readme_hashes_match_the_patch_files(self):
        readme = (PATCHES / "README.md").read_text(encoding="utf-8")
        for name in SERIES:
            self.assertIn(f"`{name}`", readme, f"{name} no está en patches/README.md")
            self.assertIn(sha256(PATCHES / name), readme, f"hash de {name} ausente o desactualizado en patches/README.md")


if __name__ == "__main__":
    unittest.main()
