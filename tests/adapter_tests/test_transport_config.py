"""Pruebas UNITARIAS de la configuración de transporte (sin Stores): validación estricta, inmutabilidad y carga desde archivo."""
import dataclasses
import json
import tempfile
import unittest
from pathlib import Path

from adapter import ConfigurationError, ProviderTimeouts, TransportConfig

ROOT = Path(__file__).resolve().parents[2]
OK = {"connect_timeout_s": 2.0, "read_timeout_s": 5.0, "total_timeout_s": 7.0}


def cfg(**providers):
    return {"providers": providers}


class ProviderTimeoutsTests(unittest.TestCase):
    def test_valid_values_are_stored_as_floats(self):
        t = ProviderTimeouts(1, 2, 3)
        self.assertEqual(t.as_dict(), {"connect_timeout_s": 1.0, "read_timeout_s": 2.0, "total_timeout_s": 3.0})
        self.assertTrue(all(isinstance(v, float) for v in t.as_dict().values()))

    def test_invalid_numbers_are_rejected(self):
        for bad in (0, -1, 0.0, float("nan"), float("inf"), float("-inf"), True, False, "5", None, [5]):
            for field in ("connect_timeout_s", "read_timeout_s", "total_timeout_s"):
                with self.subTest(v=repr(bad), field=field), self.assertRaises(ConfigurationError):
                    ProviderTimeouts(**{**OK, field: bad})

    def test_total_must_cover_connect_and_read(self):
        with self.assertRaises(ConfigurationError):
            ProviderTimeouts(connect_timeout_s=5, read_timeout_s=2, total_timeout_s=4)
        with self.assertRaises(ConfigurationError):
            ProviderTimeouts(connect_timeout_s=2, read_timeout_s=5, total_timeout_s=4)
        ProviderTimeouts(connect_timeout_s=5, read_timeout_s=5, total_timeout_s=5)  # igual vale

    def test_immutable(self):
        t = ProviderTimeouts(**OK)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            t.total_timeout_s = 99  # type: ignore[misc]


class FromDictTests(unittest.TestCase):
    def test_valid_config_and_lookup(self):
        c = TransportConfig.from_dict(cfg(**{"prov-A": OK}))
        self.assertEqual(c.timeouts_for("prov-A").total_timeout_s, 7.0)
        self.assertEqual(list(c.providers), ["prov-A"])

    def test_empty_providers_is_valid_but_nothing_resolves(self):
        c = TransportConfig.from_dict({"providers": {}})
        with self.assertRaises(ConfigurationError):
            c.timeouts_for("prov-A")

    def test_unknown_provider_has_no_default(self):
        c = TransportConfig.from_dict(cfg(**{"prov-A": OK}))
        for missing in ("prov-B", "PROV-A", "", " prov-A", None, 5, ["prov-A"]):
            with self.subTest(v=repr(missing)), self.assertRaises(ConfigurationError):
                c.timeouts_for(missing)  # type: ignore[arg-type]

    def test_schema_is_strict(self):
        bad_configs = {
            "no es objeto": [],
            "sin providers": {},
            "clave extra": {"providers": {}, "otra": 1},
            "providers no es objeto": {"providers": []},
            "proveedor no es objeto": cfg(**{"p": 5}),
            "falta un plazo": cfg(p={"connect_timeout_s": 1, "read_timeout_s": 1}),
            "plazo extra": cfg(p={**OK, "retries": 3}),
            "id vacío": cfg(**{"": OK}),
            "id con espacios en los extremos": cfg(**{" p": OK}),
            "id demasiado largo": cfg(**{"x" * 129: OK}),
            "plazo inválido": cfg(p={**OK, "read_timeout_s": -1}),
        }
        for label, data in bad_configs.items():
            with self.subTest(label), self.assertRaises(ConfigurationError):
                TransportConfig.from_dict(data)

    def test_loaded_config_is_immutable_and_isolated_from_its_source(self):
        source = cfg(**{"prov-A": dict(OK)})
        c = TransportConfig.from_dict(source)
        with self.assertRaises(TypeError):
            c.providers["otro"] = ProviderTimeouts(**OK)  # type: ignore[index]
        with self.assertRaises(dataclasses.FrozenInstanceError):
            c.providers = {}  # type: ignore[misc]
        source["providers"]["prov-A"]["total_timeout_s"] = 999
        source["providers"]["nuevo"] = dict(OK)
        self.assertEqual((c.timeouts_for("prov-A").total_timeout_s, list(c.providers)), (7.0, ["prov-A"]))


class FromFileTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)

    def write(self, text, name="t.json"):
        p = Path(self._tmp.name) / name
        p.write_text(text, encoding="utf-8")
        return p

    def test_the_example_file_stays_valid(self):
        c = TransportConfig.from_file(ROOT / "config" / "transport_config.example.json")
        self.assertEqual(sorted(c.providers), ["proveedor-a", "proveedor-b"])

    def test_valid_file(self):
        c = TransportConfig.from_file(self.write(json.dumps(cfg(**{"prov-A": OK}))))
        self.assertEqual(c.timeouts_for("prov-A").connect_timeout_s, 2.0)

    def test_missing_file(self):
        with self.assertRaises(ConfigurationError):
            TransportConfig.from_file(Path(self._tmp.name) / "no-existe.json")

    def test_invalid_json_duplicates_and_non_finite(self):
        good = '{"connect_timeout_s": 1, "read_timeout_s": 1, "total_timeout_s": 2}'
        cases = {
            "JSON roto": '{"providers": ',
            "vacío": "",
            "proveedor duplicado": '{"providers": {"p": %s, "p": %s}}' % (good, good),
            "plazo duplicado": '{"providers": {"p": {"connect_timeout_s": 1, "connect_timeout_s": 2, "read_timeout_s": 1, "total_timeout_s": 2}}}',
            "NaN": '{"providers": {"p": {"connect_timeout_s": NaN, "read_timeout_s": 1, "total_timeout_s": 2}}}',
            "Infinity": '{"providers": {"p": {"connect_timeout_s": 1, "read_timeout_s": 1, "total_timeout_s": Infinity}}}',
            "texto en vez de número": '{"providers": {"p": {"connect_timeout_s": "1", "read_timeout_s": 1, "total_timeout_s": 2}}}',
        }
        for label, text in cases.items():
            with self.subTest(label), self.assertRaises(ConfigurationError):
                TransportConfig.from_file(self.write(text))


if __name__ == "__main__":
    unittest.main()


class L3_TimeoutUpperBound(unittest.TestCase):
    """L-3 (informe g0.8 r9, D-62): ningún plazo puede superar MAX_PROVIDER_TIMEOUT_S."""

    def test_the_bound_is_accepted_and_anything_above_is_rejected(self):
        from adapter import MAX_PROVIDER_TIMEOUT_S
        self.assertEqual(MAX_PROVIDER_TIMEOUT_S, 3600.0)
        edge = {"connect_timeout_s": MAX_PROVIDER_TIMEOUT_S, "read_timeout_s": MAX_PROVIDER_TIMEOUT_S, "total_timeout_s": MAX_PROVIDER_TIMEOUT_S}
        ProviderTimeouts(**edge)
        for bad in (MAX_PROVIDER_TIMEOUT_S + 0.001, 3601, 1e12, 1e300):
            for field in ("connect_timeout_s", "read_timeout_s", "total_timeout_s"):
                with self.subTest(v=bad, field=field), self.assertRaises(ConfigurationError):
                    ProviderTimeouts(**{**{"connect_timeout_s": 1, "read_timeout_s": 1, "total_timeout_s": 1}, **({field: bad} if field == "total_timeout_s" else {field: bad, "total_timeout_s": MAX_PROVIDER_TIMEOUT_S})})

    def test_from_dict_applies_the_bound(self):
        with self.assertRaises(ConfigurationError):
            TransportConfig.from_dict(cfg(p={**OK, "total_timeout_s": 1e12}))
