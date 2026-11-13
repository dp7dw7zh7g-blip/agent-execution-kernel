"""Pruebas de la frontera de protocolo del adaptador (unitarias). AT-02, AT-03, AT-04, AT-15
y regresiones de la revisión 0.2. Las pruebas de integración (AT-01, 05-14) van en otro módulo (AT-16)."""
import json
import sqlite3
import tempfile
import unittest
import importlib.util
import uuid
from pathlib import Path

from adapter import (MAX_FENCING_TOKEN, MAX_SQLITE_INTEGER, MessageConflict, PayloadIntegrityError, ProtocolError,
                     UNAEDTOSAdapter, payload_digest, validate_envelope)
import adapter.adapter as A
from adapter.schema import SCHEMA_PATH as SCHEMA  # única referencia al esquema canónico v1.1.1 (Parte 4, Fase 3)

ROOT = Path(__file__).resolve().parents[2]


def uid():
    return str(uuid.uuid4())


def base(kind, **over):
    m = {"contract_version": "1.1", "schema_version": "1", "message_id": uid(), "message_type": kind,
         "correlation_id": "corr-1", "evidence_refs": [], "created_at": "2026-10-25T10:00:00Z"}
    m.update(over)
    return m


def _mk(kind, defaults, over):
    return base(kind, **{**defaults, **over})


def op_request(**o):
    return _mk("OPERATION_REQUEST", {"operation_id": uid()}, o)


def dispatched(**o):
    return _mk("EFFECT_DISPATCHED", {"operation_id": uid(), "effect_intent_id": uid(), "effect_attempt_id": uid(),
                                     "idempotency_key": "idem-0123456789abcdef", "fencing_token": 7}, o)


def effect_result(**o):
    return _mk("EFFECT_RESULT", {"operation_id": uid(), "effect_intent_id": uid(), "effect_attempt_id": uid(),
                                 "idempotency_key": "idem-0123456789abcdef", "result": "CONFIRMED",
                                 "evidence_refs": ["ev:1"]}, o)


def recon(**o):
    return _mk("RECONCILIATION_RESULT", {"operation_id": uid(), "effect_attempt_id": uid(), "result": "NOT_FOUND",
                                         "evidence_refs": ["ev:1"]}, o)


def recovery(**o):
    return _mk("RECOVERY_NOTICE", {"result": "RECOVERY_REQUIRED", "evidence_refs": ["ev:1"]}, o)


class Base(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db = str(Path(self._tmp.name) / "adapter.db")
        self.a = UNAEDTOSAdapter(self.db)

    def inbox_rows(self):
        c = sqlite3.connect(self.db)
        try:
            return c.execute("SELECT COUNT(*) FROM adapter_inbox").fetchone()[0]
        finally:
            c.close()


class AT02_DuplicateSamePayload(Base):
    def test_consumed_once(self):
        calls = []
        m = op_request()
        r1 = self.a.receive(m, lambda conn, msg: calls.append(msg["message_id"]))
        r2 = self.a.receive(dict(m), lambda conn, msg: calls.append(msg["message_id"]))
        self.assertEqual((r1.status, r2.status), ("ACCEPTED", "DUPLICATE"))
        self.assertEqual(r1.payload_sha256, r2.payload_sha256)
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.inbox_rows(), 1)
        self.assertEqual(self.a.quarantine_count(), 0)

    def test_processor_failure_rolls_back_inbox_row(self):
        m = op_request()

        def boom(conn, msg):
            raise RuntimeError("fallo local")
        with self.assertRaises(RuntimeError):
            self.a.receive(m, boom)
        self.assertEqual(self.inbox_rows(), 0)
        self.assertEqual(self.a.receive(m).status, "ACCEPTED")


class AT03_SameIdDifferentPayload(Base):
    def test_quarantined(self):
        m = op_request()
        self.a.receive(m)
        changed = {**m, "correlation_id": "otra"}
        with self.assertRaises(MessageConflict):
            self.a.receive(changed)
        self.assertEqual(self.a.quarantine_count(), 1)
        self.assertEqual(self.inbox_rows(), 1)


class AT04_FencingToken(Base):
    def test_invalid_values_rejected(self):
        for bad in (-1, 1.5, True, False, "7", MAX_FENCING_TOKEN + 1, MAX_SQLITE_INTEGER, [7], {"v": 7}):
            with self.subTest(token=bad):
                with self.assertRaises(ProtocolError):
                    self.a.receive({**dispatched(), "fencing_token": bad})
        self.assertEqual(self.inbox_rows(), 0)

    def test_boundaries_accepted(self):
        for ok in (0, MAX_FENCING_TOKEN):
            with self.subTest(token=ok):
                self.assertEqual(self.a.receive({**dispatched(), "fencing_token": ok}).status, "ACCEPTED")


class AT15_IncompatibleVersions(Base):
    def test_rejected(self):
        for field, val in (("contract_version", "1.0"), ("contract_version", "2.0"), ("contract_version", "1.1.1"),
                           ("schema_version", "2"), ("schema_version", 1)):
            with self.subTest(**{field: val}):
                with self.assertRaises(ProtocolError):
                    self.a.receive({**op_request(), field: val})
        self.assertEqual(self.inbox_rows(), 0)

    def test_unknown_type_rejected(self):
        with self.assertRaises(ProtocolError):
            self.a.receive({**op_request(), "message_type": "OTRO"})


class Regression_OpaqueIds(Base):
    """Los IDs son texto opaco y sensible a mayúsculas (UNAE y DTOS no generan UUID; los aportan los llamadores)."""

    def test_non_uuid_ids_accepted(self):
        for good in ("op-1", "OP-1", "res_00042", "a" * 128, uid(), "operación-ñ"):
            with self.subTest(v=good):
                validate_envelope({**op_request(), "operation_id": good})

    def test_malformed_ids_rejected(self):
        for bad in ("", " x", "x ", "a\nb", "a\x00b", "a\x7fb", "a" * 129, "e\u0301", 5, None, ["x"]):
            with self.subTest(v=repr(bad)[:20]):
                with self.assertRaises(ProtocolError):
                    validate_envelope({**op_request(), "operation_id": bad})
                with self.assertRaises(ProtocolError):
                    validate_envelope({**op_request(), "message_id": bad})

    def test_ids_are_case_sensitive_like_the_stores(self):
        a, b = op_request(message_id="msg-a"), op_request(message_id="MSG-A")
        self.assertEqual((self.a.receive(a).status, self.a.receive(b).status), ("ACCEPTED", "ACCEPTED"))
        self.assertEqual(self.inbox_rows(), 2)
        self.assertEqual(self.a.receive(dict(a)).status, "DUPLICATE")  # igualdad exacta

    def test_correlation_with_opaque_ids_and_exact_match(self):
        kw = dict(provider="p", idempotency_key="k" * 16, fencing_token=1, intent_payload={})
        self.a.bind_correlation(operation_id="op-1", effect_intent_id="int-1", effect_attempt_id="att-1", **kw)
        self.a.bind_correlation(operation_id="op-1", effect_intent_id="int-1", effect_attempt_id="att-1", **kw)  # idempotente
        self.assertIsNone(self.a.get_correlation("OP-1"))
        with self.assertRaises(ProtocolError):
            self.a.bind_correlation(operation_id=" op-2", effect_intent_id="int-2", effect_attempt_id="att-2", **kw)

    def test_correlation_conflict_and_idempotent_rebind(self):
        op, it, at = uid(), uid(), uid()
        kw = dict(provider="p", idempotency_key="k" * 16, fencing_token=1, intent_payload={"a": 1})
        self.a.bind_correlation(operation_id=op, effect_intent_id=it, effect_attempt_id=at, **kw)
        self.a.bind_correlation(operation_id=op, effect_intent_id=it, effect_attempt_id=at, **kw)  # idempotente
        with self.assertRaises(MessageConflict):
            self.a.bind_correlation(operation_id=op, effect_intent_id=uid(), effect_attempt_id=uid(), **kw)
        with self.assertRaises(MessageConflict):  # mismo intento, otra operación
            self.a.bind_correlation(operation_id=uid(), effect_intent_id=uid(), effect_attempt_id=at, **kw)
        self.assertEqual(self.a.get_correlation(op)["effect_intent_id"], it)


class Regression_PayloadSha256(Base):
    def test_hash_excludes_own_field(self):
        m = op_request()
        d = validate_envelope(m)
        self.assertEqual(validate_envelope({**m, "payload_sha256": d}), d)
        self.assertEqual(validate_envelope({**m, "payload_sha256": d.upper()}), d)
        self.assertEqual(payload_digest({**m, "payload_sha256": d}), d)

    def test_with_and_without_field_are_same_message(self):
        m = op_request()
        d = validate_envelope(m)
        self.assertEqual(self.a.receive(m).status, "ACCEPTED")
        self.assertEqual(self.a.receive({**m, "payload_sha256": d}).status, "DUPLICATE")

    def test_mismatch_is_quarantined_and_not_stored(self):
        m = op_request()
        with self.assertRaises(PayloadIntegrityError):
            self.a.receive({**m, "payload_sha256": "0" * 64})
        self.assertEqual(self.a.quarantine_count(), 1)
        self.assertEqual(self.inbox_rows(), 0)

    def test_malformed_hash_rejected(self):
        for bad in ("abc", "g" * 64, 5):
            with self.subTest(value=bad):
                with self.assertRaises(ProtocolError):
                    validate_envelope({**op_request(), "payload_sha256": bad})


class Regression_SchemaStrictness(Base):
    def test_valid_messages_per_type(self):
        for make in (op_request, dispatched, effect_result, recon, recovery):
            with self.subTest(make=make.__name__):
                self.assertEqual(self.a.receive(make()).status, "ACCEPTED")

    def test_recovery_notice_operation_id_optional_evidence_required(self):
        self.assertEqual(self.a.receive(recovery()).status, "ACCEPTED")
        self.assertEqual(self.a.receive(recovery(operation_id=uid())).status, "ACCEPTED")
        with self.assertRaises(ProtocolError):
            self.a.receive(recovery(evidence_refs=[]))

    def test_reconciliation_requires_attempt_and_evidence(self):
        m = recon()
        del m["effect_attempt_id"]
        with self.assertRaises(ProtocolError):
            validate_envelope(m)
        with self.assertRaises(ProtocolError):
            validate_envelope(recon(evidence_refs=[]))

    def test_unknown_field_result_and_duplicates_rejected(self):
        for label, m in (("extra", {**op_request(), "campo_extra": 1}),
                         ("result", effect_result(result="XYZ")),
                         ("dup_refs", effect_result(evidence_refs=["a", "a"])),
                         ("blank_corr", {**op_request(), "correlation_id": "  "}),
                         ("naive_date", {**op_request(), "created_at": "2026-10-25T10:00:00"}),
                         ("basic_date", {**op_request(), "created_at": "20261025"}),
                         ("not_dict", [1])):
            with self.subTest(case=label):
                with self.assertRaises(ProtocolError):
                    validate_envelope(m)

    def test_not_found_is_a_valid_result_value(self):
        # I-07/AT-11: el adaptador no reinterpreta NOT_FOUND; solo lo transporta.
        self.assertEqual(self.a.receive(recon(result="NOT_FOUND")).status, "ACCEPTED")


class SchemaAlignment(unittest.TestCase):
    """Detecta deriva entre las constantes del adaptador y el esquema 1.1.1 (sin dependencias)."""

    @classmethod
    def setUpClass(cls):
        cls.schema = json.loads(SCHEMA.read_text(encoding="utf-8"))

    def test_fields_and_base_required(self):
        self.assertEqual(set(self.schema["properties"]), set(A.ENVELOPE_FIELDS))
        self.assertEqual(set(self.schema["required"]), set(A.BASE_REQUIRED))
        self.assertIs(self.schema["additionalProperties"], False)

    def test_fencing_token_maximum_matches_the_adapter(self):
        self.assertEqual(self.schema["properties"]["fencing_token"]["maximum"], MAX_FENCING_TOKEN)
        for blk in self.schema["allOf"]:
            ft = blk["then"].get("properties", {}).get("fencing_token")
            if ft is not None:
                self.assertEqual(ft["maximum"], MAX_FENCING_TOKEN)

    def test_result_enum_and_types(self):
        enum = set(self.schema["properties"]["result"]["enum"]) - {None}
        self.assertEqual(enum, set(A.RESULT_VALUES))
        self.assertEqual(set(self.schema["properties"]["message_type"]["enum"]), set(A.MESSAGE_TYPES))

    def test_per_type_required_and_evidence(self):
        for blk in self.schema["allOf"]:
            kind = blk["if"]["properties"]["message_type"]["const"]
            self.assertEqual(set(blk["then"]["required"]), set(A.PER_TYPE_REQUIRED[kind]), kind)
            nonempty = blk["then"].get("properties", {}).get("evidence_refs", {}).get("minItems", 0) >= 1
            self.assertEqual(nonempty, kind in A.EVIDENCE_NONEMPTY, kind)
        self.assertEqual({b["if"]["properties"]["message_type"]["const"] for b in self.schema["allOf"]} | {"OPERATION_REQUEST"},
                         set(A.MESSAGE_TYPES))

    def test_id_constraints_match_adapter(self):
        for f in A.ID_FIELDS:
            prop = self.schema["properties"][f]
            self.assertEqual(prop["maxLength"], A.MAX_ID_LENGTH, f)
            self.assertEqual(prop["pattern"], "^\\S(?:.*\\S)?$", f)


def _jcs_string(s):
    """Cadena como JSON.stringify de ECMAScript (RFC 8785 §3.2.2.2)."""
    escapes = {'"': '\\"', "\\": "\\\\", "\b": "\\b", "\f": "\\f", "\n": "\\n", "\r": "\\r", "\t": "\\t"}
    return '"' + "".join(escapes.get(c) or (f"\\u{ord(c):04x}" if ord(c) < 0x20 else c) for c in s) + '"'


def _jcs(v):
    """Referencia RFC 8785 independiente, SOLO para valores del sobre: null, bool, enteros seguros, cadenas, listas y objetos."""
    if v is None:
        return "null"
    if v is True:
        return "true"
    if v is False:
        return "false"
    if isinstance(v, int):
        assert abs(v) <= MAX_FENCING_TOKEN, "fuera del rango exacto de un double: JCS escribiría otra cosa"
        return str(v)
    if isinstance(v, str):
        return _jcs_string(v)
    if isinstance(v, list):
        return "[" + ",".join(_jcs(x) for x in v) + "]"
    return "{" + ",".join(_jcs_string(k) + ":" + _jcs(v[k]) for k in sorted(v, key=lambda k: k.encode("utf-16-be"))) + "}"


class JcsCompliance(unittest.TestCase):
    """Qué parte de RFC 8785 cumple el canonicalizador del adaptador y cuál NO. Conclusión (D-41): para el SOBRE cumple, porque sus claves son
    ASCII fijas, sus valores son cadenas/listas/enteros y el fencing_token está acotado a 2^53-1. Para JSON arbitrario NO es JCS."""

    def test_envelope_keys_are_ascii(self):
        self.assertTrue(all(k.isascii() for k in A.ENVELOPE_FIELDS))

    def test_matches_reference_jcs_for_envelope_shaped_values(self):
        import random
        rng = random.Random(8785)
        pool = ["a", "Z", "0", " ", "ñ", "日本語", "\u20ac", "\ufb33", "\U0001f600", "\u2028", '"', "\\", "\n", "\t", "\x00", "\x1f", "\x7f", "/"]
        ints = [0, 1, 7, 255, 2 ** 31, 2 ** 53 - 1]

        def word():
            return "".join(rng.choice(pool) for _ in range(rng.randint(0, 8)))

        def value(depth=0):
            kind = rng.randint(0, 5 if depth < 2 else 3)
            if kind == 0:
                return None
            if kind == 1:
                return rng.choice(ints)
            if kind == 2:
                return word()
            if kind == 3:
                return rng.choice([True, False])
            if kind == 4:
                return [value(depth + 1) for _ in range(rng.randint(0, 3))]
            return {k: value(depth + 1) for k in rng.sample(sorted(A.ENVELOPE_FIELDS), rng.randint(0, 4))}
        for i in range(300):
            v = {k: value() for k in rng.sample(sorted(A.ENVELOPE_FIELDS), rng.randint(1, 8))}
            self.assertEqual(A.canonical_json(v), _jcs(v), f"caso {i}")

    def test_known_limitation_key_order_for_non_bmp_keys(self):
        """RFC 8785 §3.2.3: JCS ordena por unidades UTF-16; Python por punto de código. Difieren para claves fuera del plano básico."""
        d = {"\u20ac": "Euro Sign", "\r": "Carriage Return", "\ufb33": "Hebrew Letter Dalet With Dagesh", "1": "One",
             "\U0001f600": "Emoji: Grinning Face", "\u0080": "Control", "\u00f6": "Latin Small Letter O With Diaeresis"}
        jcs_order = list(json.loads(_jcs(d), object_pairs_hook=lambda p: dict(p)))
        ours = list(json.loads(A.canonical_json(d), object_pairs_hook=lambda p: dict(p)))
        self.assertEqual(jcs_order, ["\r", "1", "\u0080", "\u00f6", "\u20ac", "\U0001f600", "\ufb33"])
        self.assertEqual(ours, ["\r", "1", "\u0080", "\u00f6", "\u20ac", "\ufb33", "\U0001f600"])
        self.assertNotEqual(jcs_order, ours)

    def test_known_limitation_large_integers_are_not_jcs_but_the_wire_never_carries_them(self):
        """JCS escribe un entero > 2^53 como el double más cercano (9223372036854775807 -> 9223372036854776000); Python lo deja exacto."""
        self.assertEqual(A.canonical_json({"t": 2 ** 63 - 1}), '{"t":9223372036854775807}')
        with self.assertRaises(ProtocolError):
            validate_envelope({**dispatched(), "fencing_token": 2 ** 53})  # el sobre lo rechaza: nunca llega al canonicalizador
        self.assertEqual(A.canonical_json({"t": MAX_FENCING_TOKEN}), '{"t":9007199254740991}')

    def test_floats_are_never_part_of_the_envelope(self):
        for bad in (1.0, 1.5):
            with self.assertRaises(ProtocolError):
                validate_envelope({**dispatched(), "fencing_token": bad})


class CanonicalParity(unittest.TestCase):
    """El canonicalizador del adaptador produce el mismo texto que el de DTOS para entradas finitas; difiere solo en que NO admite NaN/Infinity."""

    CORPUS = [{"b": [1, {"z": "ñandú", "a": None}], "a": 2 ** 62}, {"k": "日本語 \u00e9", "n": [True, False, 0, -1, 1.5]},
              {}, [], {"x": {"y": {"z": [1, 2, 3]}}}, {"max": MAX_SQLITE_INTEGER}]

    def test_same_text_as_dtos(self):
        from dtos.canonical import canonical_json as dtos_canonical
        for v in self.CORPUS:
            with self.subTest(v=str(v)[:30]):
                self.assertEqual(A.canonical_json(v), dtos_canonical(v))

    def test_adapter_rejects_non_finite_numbers(self):
        for bad in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(v=bad), self.assertRaises(ValueError):
                A.canonical_json({"x": bad})


@unittest.skipUnless(importlib.util.find_spec("jsonschema"), "jsonschema no instalado")
class SchemaConformance(unittest.TestCase):
    """El adaptador y el esquema coinciden en el veredicto para un corpus (solo con jsonschema)."""

    def test_corpus(self):
        import jsonschema
        v = jsonschema.Draft202012Validator(json.loads(SCHEMA.read_text(encoding="utf-8")))
        bad_recon = recon(); del bad_recon["effect_attempt_id"]
        corpus = [op_request(), dispatched(), effect_result(), recon(), recovery(), recovery(operation_id=uid()),
                  {**dispatched(), "fencing_token": -1}, {**dispatched(), "fencing_token": "7"}, {**dispatched(), "fencing_token": True},
                  {**op_request(), "message_id": " " + uid()}, {**op_request(), "campo_extra": 1},
                  effect_result(result="XYZ"), effect_result(evidence_refs=["a", "a"]), recovery(evidence_refs=[]),
                  bad_recon, {**op_request(), "correlation_id": "  "}, {**op_request(), "payload_sha256": "xyz"}]
        for i, m in enumerate(corpus):
            try:
                validate_envelope(m); adapter_ok = True
            except ProtocolError:
                adapter_ok = False
            self.assertEqual(adapter_ok, v.is_valid(m), f"caso {i}: {m}")


if __name__ == "__main__":
    unittest.main()


class A2_EnvelopeBounds(Base):
    """A-2 (informe g0.8 r9, D-60): cotas en bytes UTF-8, surrogates y caracteres de control; siempre ProtocolError y antes del digest."""

    def rejected(self, m):
        with self.assertRaises(ProtocolError):
            validate_envelope(m)
        with self.assertRaises(ProtocolError):  # receive también: sin UnicodeEncodeError ni fila en el inbox
            self.a.receive(m)
        self.assertEqual(self.a.quarantine_count(), 0)  # un mensaje inválido no ensucia la cuarentena

    def test_lone_surrogates_are_a_protocol_error_in_every_text_field(self):
        for label, m in (("correlation_id", effect_result(correlation_id="a\ud800b")),
                         ("message_id", effect_result(message_id="a\udfffb")),
                         ("operation_id", effect_result(operation_id="x\ud800")),
                         ("idempotency_key", effect_result(idempotency_key="idem-\ud800-0123456789")),
                         ("authority_ref", effect_result(authority_ref="auth\ud800")),
                         ("evidence_refs", effect_result(evidence_refs=["ev\ud800"]))):
            with self.subTest(field=label):
                self.rejected(m)

    def test_correlation_id_cap_is_in_bytes_not_characters(self):
        validate_envelope(effect_result(correlation_id="a" * A.MAX_CORRELATION_ID_BYTES))
        self.rejected(effect_result(correlation_id="a" * (A.MAX_CORRELATION_ID_BYTES + 1)))
        validate_envelope(effect_result(correlation_id="ñ" * (A.MAX_CORRELATION_ID_BYTES // 2)))  # 256 bytes exactos
        self.rejected(effect_result(correlation_id="ñ" * (A.MAX_CORRELATION_ID_BYTES // 2) + "a"))  # 129 caracteres, 257 bytes
        self.rejected(effect_result(correlation_id="x" * 5_000_000))

    def test_evidence_refs_count_cap(self):
        validate_envelope(effect_result(evidence_refs=[f"e{i}" for i in range(A.MAX_EVIDENCE_REFS)]))
        self.rejected(effect_result(evidence_refs=[f"e{i}" for i in range(A.MAX_EVIDENCE_REFS + 1)]))
        self.rejected(effect_result(evidence_refs=[f"e{i}" for i in range(200_000)]))

    def test_evidence_ref_size_cap_is_in_bytes(self):
        validate_envelope(effect_result(evidence_refs=["a" * A.MAX_EVIDENCE_REF_BYTES]))
        self.rejected(effect_result(evidence_refs=["a" * (A.MAX_EVIDENCE_REF_BYTES + 1)]))
        validate_envelope(effect_result(evidence_refs=["ñ" * (A.MAX_EVIDENCE_REF_BYTES // 2)]))
        self.rejected(effect_result(evidence_refs=["ñ" * (A.MAX_EVIDENCE_REF_BYTES // 2) + "a"]))

    def test_evidence_refs_reject_control_characters(self):
        for bad in ("a\x00b", "a\nb", "\x7f", "a\tb"):
            with self.subTest(v=repr(bad)):
                self.rejected(effect_result(evidence_refs=[bad]))

    def test_envelope_total_cap_is_defense_in_depth_and_the_worst_valid_envelope_fits(self):
        """Con todos los campos de texto acotados, el mayor sobre válido (64 referencias de 1.024 bytes + textos de 256 bytes) cabe con holgura;
        el tope de 128 KiB solo actúa si alguien sube otra cota. Se comprueba el mecanismo bajando el tope."""
        worst = effect_result(evidence_refs=[f"{i:03d}" + "x" * (A.MAX_EVIDENCE_REF_BYTES - 3) for i in range(A.MAX_EVIDENCE_REFS)],
                              correlation_id="c" * 256, cognitive_cycle_id="g" * 256, authority_ref="a" * 256, idempotency_key="k" * 256)
        validate_envelope(worst)
        size = len(A.canonical_json(worst).encode("utf-8"))
        self.assertLess(size, A.MAX_ENVELOPE_BYTES // 1.5)
        original = A.MAX_ENVELOPE_BYTES
        A.MAX_ENVELOPE_BYTES = size - 1
        try:
            self.rejected(worst)
            A.MAX_ENVELOPE_BYTES = size
            validate_envelope(worst)  # justo en el tope
        finally:
            A.MAX_ENVELOPE_BYTES = original

    def test_bounds_are_checked_before_the_digest_is_computed(self):
        calls = []
        original = A.payload_digest
        A.payload_digest = lambda m: calls.append(1) or original(m)
        try:
            for m in (effect_result(correlation_id="a\ud800"), effect_result(evidence_refs=["e\x00"]), effect_result(authority_ref="a" * 5_000_000)):
                with self.assertRaises(ProtocolError):
                    validate_envelope(m)
        finally:
            A.payload_digest = original
        self.assertEqual(calls, [])

    def test_the_new_caps_are_exported_and_consistent(self):
        import adapter
        for name in ("MAX_ENVELOPE_BYTES", "MAX_CORRELATION_ID_BYTES", "MAX_EVIDENCE_REFS", "MAX_EVIDENCE_REF_BYTES"):
            self.assertIn(name, adapter.__all__)
        self.assertEqual((A.MAX_ENVELOPE_BYTES, A.MAX_CORRELATION_ID_BYTES, A.MAX_EVIDENCE_REFS, A.MAX_EVIDENCE_REF_BYTES), (131072, 256, 64, 1024))
        self.assertEqual(A.MAX_ENVELOPE_BYTES, 2 * A.MAX_EVIDENCE_SIZE_BYTES)


class C_TextFieldsAndIdempotencyKey(Base):
    """D-62 (g0.8 r12): los cuatro campos de texto del sobre comparten tope (256 bytes UTF-8) y prohibición de caracteres de control; L-2: idempotency_key."""
    FIELDS = ("correlation_id", "cognitive_cycle_id", "idempotency_key", "authority_ref")

    def rejected(self, m):
        with self.assertRaises(ProtocolError):
            validate_envelope(m)
        with self.assertRaises(ProtocolError):
            self.a.receive(m)

    def test_cap_in_bytes_for_every_text_field(self):
        for f in self.FIELDS:
            with self.subTest(field=f):
                validate_envelope(effect_result(**{f: "a" * 256}))
                validate_envelope(effect_result(**{f: "ñ" * 128}))   # 256 bytes exactos
                self.rejected(effect_result(**{f: "a" * 257}))
                self.rejected(effect_result(**{f: "ñ" * 128 + "a"}))  # 129 caracteres, 257 bytes
                self.rejected(effect_result(**{f: "x" * 5_000_000}))

    def test_control_characters_are_rejected_in_every_text_field(self):
        for f in self.FIELDS:
            for bad in ("a\x00b", "a\nb", "a\tb", "a\x7fb", "\x1b[31m"):
                with self.subTest(field=f, v=repr(bad)):
                    self.rejected(effect_result(**{f: "ok-0123456789abcdef" + bad}))

    def test_surrogates_are_rejected_in_every_text_field(self):
        for f in self.FIELDS:
            with self.subTest(field=f):
                self.rejected(effect_result(**{f: "a\ud800b"}))

    def test_idempotency_key_has_no_edge_spaces_in_the_envelope(self):
        validate_envelope(effect_result(idempotency_key="idem 0123 4567 89ab"))  # espacios internos: válidos
        for bad in (" idem-0123456789abcdef", "idem-0123456789abcdef ", " " * 16):
            with self.subTest(v=repr(bad)):
                self.rejected(effect_result(idempotency_key=bad))

    def test_bind_correlation_applies_the_same_rules_as_the_envelope(self):
        kw = dict(provider="prov-A", fencing_token=1, intent_payload={"n": 1})
        for n, bad in enumerate((" " * 16, " idem-0123456789abcdef", "idem-0123456789abcdef ", "idem-0123456789\x00abcdef", "idem-0123456789\nabcdef",
                                 "idem-0123456789\ud800abcdef", "k" * 257, "ñ" * 129)):
            with self.subTest(v=repr(bad)[:24]), self.assertRaises(ProtocolError):
                self.a.bind_correlation(operation_id=f"op-{n}", effect_intent_id=f"int-{n}", effect_attempt_id=f"att-{n}", idempotency_key=bad, **kw)
        self.a.bind_correlation(operation_id="op-ok", effect_intent_id="int-ok", effect_attempt_id="att-ok", idempotency_key="k" * 256, **kw)  # 256 bytes: válida
        self.assertEqual(self.a.list_correlations()[0]["operation_id"], "op-ok")  # las rechazadas no dejaron nada

    def test_the_text_field_cap_is_exported_and_equals_the_correlation_cap(self):
        import adapter
        self.assertIn("MAX_TEXT_FIELD_BYTES", adapter.__all__)
        self.assertEqual(A.MAX_TEXT_FIELD_BYTES, A.MAX_CORRELATION_ID_BYTES)
