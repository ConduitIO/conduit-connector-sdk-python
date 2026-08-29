"""Cross-language Avro wire-compatibility tests for `conduit.schema.AvroSchema`.

The property that matters for WS2 is **wire compatibility** with
`conduit-commons`' `schema/avro` package (Go), not merely "this Python code
can read its own output" -- a Python-only round-trip is not evidence of
that. This module proves it against `tests/testdata/avro_golden.json`:

- ``go_avro_hex`` per case: bytes `github.com/iskorotkov/avro/v2` (the
  library `conduit-commons` wraps, post-#279) actually produces today.
  `tools/avro_fixture_gen`'s verify mode re-derives them live and fails if
  the committed bytes drift, so they cannot silently go stale.
- ``python_avro_hex`` per case: bytes `AvroSchema.encode` produces, pinned
  byte-exactly here *and* verified Go-decodable -- the Go verifier decodes
  them to the expected value, and this module re-runs that verifier live
  whenever a Go toolchain is available.
- ``LIVE_HEX`` per nondeterministic case: bytes the Go verifier marshals
  fresh at verify time (the `with_map` case -- a non-empty map field has no
  stable Go bytes, since Go's `avro.Marshal` iterates map entries in
  randomized order); this module decodes them back to the value, the
  cross-language direction that is always pinnable.

Bytes-typed fields carry their content as a UTF-8 string in the fixture's
``value`` (JSON has no bytes literal); `_encode_value` converts the fields
listed in the case's ``bytes_fields``, and every assertion compares against
the converted value.

See `conduit/schema.py`'s module docstring for the full contract and its
documented limits (array/map block-framing style, not byte-identical in one
direction for non-empty arrays/maps -- still mutually decodable; map fields
nondeterministic on the Go side by construction, pinned only for the
Python direction plus live-Go decode).
"""

from __future__ import annotations

import importlib
import json
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

import conduit.schema as schema_module
from conduit.record import Metadata
from conduit.schema import AvroSchema

_GOLDEN_PATH = Path(__file__).parent / "testdata" / "avro_golden.json"
_GOLDEN: dict[str, Any] = json.loads(_GOLDEN_PATH.read_text())


def _schema_for(case: dict[str, Any]) -> AvroSchema:
    """A case's schema: its own ``schema_json`` if present, else the fixture-level one."""
    return AvroSchema(case.get("schema_json") or _GOLDEN["schema_json"])


def _encode_value(case: dict[str, Any]) -> dict[str, Any]:
    """The value to pass to ``encode``: ``value`` with bytes fields UTF-8-converted."""
    value = dict(case["value"])
    for field in case.get("bytes_fields", []):
        value[field] = value[field].encode("utf-8")
    return value


@pytest.mark.parametrize("case", _GOLDEN["cases"], ids=lambda c: c["name"])
class TestPythonEncodingIsPinnedToGoVerifiedBytes:
    """Python's encode output is pinned to bytes Go has verified it can read.

    ``python_avro_hex`` was produced by this SDK's own encoder and verified
    by `tools/avro_fixture_gen` (Go decodes it to the expected value). Pinning
    it here makes the Python side of the cross-language proof run in
    Python-only CI -- any change to the encoder that alters the bytes (or to
    the pinned values) fails loudly instead of drifting.
    """

    def test_encode_matches_committed_go_verified_bytes_exactly(self, case: dict[str, Any]) -> None:
        encoded = _schema_for(case).encode(_encode_value(case))
        assert encoded == bytes.fromhex(case["python_avro_hex"]), (
            "encode() output drifted from the Go-verified python_avro_hex; "
            "re-verify with tools/avro_fixture_gen before committing new bytes"
        )

    def test_round_trips_through_this_sdk(self, case: dict[str, Any]) -> None:
        schema = _schema_for(case)
        encoded = schema.encode(_encode_value(case))
        assert schema.decode(encoded) == _encode_value(case)


@pytest.mark.parametrize(
    "case", [c for c in _GOLDEN["cases"] if c.get("go_avro_hex")], ids=lambda c: c["name"]
)
class TestGoProducedBytesDecodeCorrectlyInPython:
    """The direction that matters most: can this SDK read what Go wrote?

    Every case is byte-exact-verified for this direction implicitly (the
    golden bytes decode to exactly the expected value), regardless of the
    framing caveats -- decoding handles both block-framing styles per the
    Avro spec, which is exactly why this direction has no exceptions. The
    ``with_map`` case is excluded: a non-empty map field has no stable Go
    bytes to pin (see ``TestMapFieldCase`` and the live-Go test below).
    """

    def test_decodes_to_the_expected_value(self, case: dict[str, Any]) -> None:
        golden_bytes = bytes.fromhex(case["go_avro_hex"])
        decoded = _schema_for(case).decode(golden_bytes)
        assert decoded == _encode_value(case)


@pytest.mark.parametrize(
    "case",
    [c for c in _GOLDEN["cases"] if c["byte_exact_encode_both_directions"]],
    ids=lambda c: c["name"],
)
def test_byte_exact_bidirectional_cases_match_go_exactly(case: dict[str, Any]) -> None:
    """For cases without framing ambiguity, bytes match Go exactly, not just values.

    Covers every scalar type (int/long/float/double/string/boolean/bytes),
    nested records, unions, and empty arrays/maps -- the byte-exact
    evidence behind the "same bytes both directions" claim.
    """
    schema = _schema_for(case)
    golden_bytes = bytes.fromhex(case["go_avro_hex"])
    encoded = schema.encode(_encode_value(case))
    assert encoded == golden_bytes, (
        "expected byte-exact match with Go's iskorotkov/avro output for a case "
        "explicitly marked unambiguous (no non-empty array/map field encoding choice)"
    )


@pytest.mark.parametrize(
    "case",
    [
        c
        for c in _GOLDEN["cases"]
        if not c["byte_exact_encode_both_directions"] and c.get("go_avro_hex")
    ],
    ids=lambda c: c["name"],
)
def test_non_byte_exact_case_is_decode_compatible_but_not_byte_identical_on_encode(
    case: dict[str, Any],
) -> None:
    """Documents (and pins) the real, honest divergence -- see the module docstring.

    If this ever starts passing (bytes become identical), the encoders
    converged and the golden fixture's flags -- and this test -- should be
    updated, not silently left stale.
    """
    schema = _schema_for(case)
    golden_bytes = bytes.fromhex(case["go_avro_hex"])
    encoded = schema.encode(_encode_value(case))
    assert encoded != golden_bytes, (
        "expected the documented block-framing byte difference; if this now "
        "matches, update the golden fixture's byte_exact_encode_both_directions flag"
    )
    # But both still decode to the identical value -- that's the property
    # that actually matters (see module docstring).
    expected = _encode_value(case)
    assert schema.decode(golden_bytes) == schema.decode(encoded) == expected


class TestMapFieldCase:
    """Map fields: decode-compatible both directions; Go bytes unpinnable by construction.

    Go's ``avro.Marshal`` iterates map entries in randomized order, so the
    ``with_map`` case pins only ``python_avro_hex`` (fastavro iterates dicts
    in insertion order -- Python bytes are stable). What IS verified for the
    reverse direction: the Go verifier decodes those Python bytes, and the
    live-Go test below decodes Go's freshly-marshaled map bytes in Python --
    decoding is order-independent in both codecs, which is the property that
    matters.
    """

    @staticmethod
    def _case() -> dict[str, Any]:
        return next(c for c in _GOLDEN["cases"] if c["name"] == "with_map")

    def test_python_encode_matches_pinned_bytes(self) -> None:
        case = self._case()
        encoded = _schema_for(case).encode(_encode_value(case))
        assert encoded == bytes.fromhex(case["python_avro_hex"])

    def test_python_round_trip(self) -> None:
        case = self._case()
        schema = _schema_for(case)
        assert schema.decode(schema.encode(_encode_value(case))) == _encode_value(case)

    def test_fixture_flags_the_case_as_go_side_nondeterministic(self) -> None:
        """The unpinnable flag must stay explicit so it can't silently go stale."""
        case = self._case()
        assert case.get("go_side_nondeterministic") is True
        assert not case["byte_exact_encode_both_directions"]
        assert not case.get("go_avro_hex")


@pytest.mark.timeout(180)  # first `go run` downloads the module cache; overrides the 60s global
def test_go_verifier_confirms_python_bytes_decode_in_go() -> None:
    """Re-run the Go verifier live: Go decodes this SDK's pinned bytes.

    ``tools/avro_fixture_gen`` re-derives ``go_avro_hex`` from the committed
    values and decodes ``python_avro_hex`` (this SDK's encode output) back
    to the expected values, using the exact Avro library conduit-commons
    wraps. For ``go_side_nondeterministic`` cases it marshals the value
    live and prints ``LIVE_HEX`` -- those genuinely-Go-produced bytes are
    then decoded right here in Python, completing the cross-language loop
    for the one case class whose Go bytes can't be pinned. Skipped when no
    Go toolchain is present (the committed fixture plus the pinned-bytes
    tests above carry the proof in Python-only CI); when Go exists, a
    failure here is a real cross-language regression.
    """
    go = shutil.which("go")
    if go is None:
        pytest.skip("no Go toolchain available; fixture carries the Go verification")
    tool_dir = Path(__file__).parents[1] / "tools" / "avro_fixture_gen"
    result = subprocess.run(
        [go, "run", ".", "-fixture", str(_GOLDEN_PATH)],
        cwd=tool_dir,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert result.returncode == 0, (
        "Go verifier failed -- Go cannot reproduce go_avro_hex or cannot decode "
        f"python_avro_hex. Output:\n{result.stdout}\n{result.stderr}"
    )
    live_hexes: dict[str, bytes] = {}
    for line in result.stdout.splitlines():
        if line.startswith("LIVE_HEX "):
            _, name, hexed = line.split(" ", 2)
            live_hexes[name] = bytes.fromhex(hexed)
    assert live_hexes, "expected at least one LIVE_HEX (the with_map case) from the verifier"
    for name, go_bytes in live_hexes.items():
        case = next(c for c in _GOLDEN["cases"] if c["name"] == name)
        # Go's live bytes (map entry order randomized on the Go side) still
        # decode to the exact same value here -- decode is order-independent.
        assert _schema_for(case).decode(go_bytes) == _encode_value(case)


class TestSchemaRegistrationAndMetadataContract:
    """The author-facing "registration" flow the design doc §2.6 specifies.

    No schema registry client exists in this SDK (documented scope
    boundary): "registration" is the author supplying schema text, encoding
    with :class:`AvroSchema`, and recording the schema reference as
    ``opencdc.{key,payload}.schema.{subject,version}`` metadata via
    :class:`conduit.record.Metadata`. These tests pin that contract,
    including that the reference lives in metadata, not in the bytes.
    """

    def test_register_encode_and_attach_metadata_end_to_end(self) -> None:
        """The full author flow: parse, encode a record, attach subject/version."""
        value = _encode_value(_GOLDEN["cases"][0])
        metadata: dict[str, str] = {}
        Metadata.set_payload_schema(
            metadata, subject="io.conduit.example.OpenCDCPayload", version=1
        )

        encoded = _schema_for(_GOLDEN["cases"][0]).encode(value)

        # The bytes are exactly what Go produces/accepts for this value --
        # and the schema reference that lets a downstream consumer decode
        # them is in the metadata, not embedded in the bytes.
        assert Metadata.get_payload_schema(metadata) == (
            "io.conduit.example.OpenCDCPayload",
            1,
        )
        assert _schema_for(_GOLDEN["cases"][0]).decode(encoded) == value

    def test_the_schema_reference_is_not_embedded_in_the_bytes(self) -> None:
        """Same value + same schema, different subject/version -> identical bytes.

        This is the design doc's schema-id behavior stated as a property:
        the plain (headerless) Avro wire format has no schema-ID header, so
        changing the metadata reference must not change the encoding --
        there is nothing in the bytes for it to change.
        """
        case = _GOLDEN["cases"][0]
        value = _encode_value(case)
        schema = _schema_for(case)
        encoded = schema.encode(value)
        for subject, version in (("a.subject", 1), ("a.subject", 2)):
            metadata: dict[str, str] = {}
            Metadata.set_payload_schema(metadata, subject=subject, version=version)
            # The metadata reference round-trips...
            assert Metadata.get_payload_schema(metadata) == (subject, version)
            # ...and the encoded bytes are unchanged by which reference is used.
            assert schema.encode(value) == encoded
        # No reference of any kind appears in the encoded bytes.
        assert b"subject" not in encoded and b"OpenCDCPayload" not in encoded

    def test_metadata_accessors_require_both_subject_and_version(self) -> None:
        metadata: dict[str, str] = {}
        assert Metadata.get_payload_schema(metadata) is None

        Metadata.set_payload_schema(metadata, subject="s", version=1)
        # A subject without a version is not a valid reference.
        del metadata[Metadata.PAYLOAD_SCHEMA_VERSION]
        assert Metadata.get_payload_schema(metadata) is None
        assert metadata[Metadata.PAYLOAD_SCHEMA_SUBJECT] == "s"  # raw key still readable

        metadata = {}
        Metadata.set_key_schema(metadata, subject="k", version=3)
        assert Metadata.get_key_schema(metadata) == ("k", 3)
        del metadata[Metadata.KEY_SCHEMA_SUBJECT]
        assert Metadata.get_key_schema(metadata) is None


def _schema_with_fields(*fields: dict[str, Any]) -> AvroSchema:
    return AvroSchema(json.dumps({"type": "record", "name": "R", "fields": list(fields)}))


class TestEncodeValidationIsStrict:
    """encode() rejects silent coercion -- the invariant-6 contract.

    fastavro's own writer silently coerces several of these (``42.9`` into
    a long writes ``42``, ``True`` into a long writes ``1``, extra dict
    keys are dropped); the Go codec conduit-commons wraps rejects them
    ("avro: float64 is unsupported for Avro long", "avro: bool is
    unsupported for Avro long", ...). encode() must behave like the Go
    codec, not like fastavro's lax writer: a connector whose data drifts
    type-wise fails loudly at encode time, never writes a *different value*
    downstream.
    """

    def test_float_into_long_field_raises_type_error(self) -> None:
        schema = _schema_with_fields({"name": "id", "type": "long"})
        with pytest.raises(TypeError, match="float is unsupported for Avro long"):
            schema.encode({"id": 42.9})

    def test_bool_into_long_field_raises_type_error(self) -> None:
        schema = _schema_with_fields({"name": "id", "type": "long"})
        with pytest.raises(TypeError, match="bool is unsupported for Avro long"):
            schema.encode({"id": True})

    def test_float_into_long_inside_array_raises_type_error(self) -> None:
        schema = _schema_with_fields({"name": "nums", "type": {"type": "array", "items": "long"}})
        with pytest.raises(TypeError, match=r'field "nums\[1\]".*unsupported for Avro long'):
            schema.encode({"nums": [1, 2.5]})

    def test_int_into_double_field_raises_type_error(self) -> None:
        schema = _schema_with_fields({"name": "score", "type": "double"})
        with pytest.raises(TypeError, match="int is unsupported for Avro double"):
            schema.encode({"score": 42})

    def test_bool_into_double_field_raises_type_error(self) -> None:
        schema = _schema_with_fields({"name": "score", "type": "double"})
        with pytest.raises(TypeError, match="bool is unsupported for Avro double"):
            schema.encode({"score": True})

    def test_unknown_field_raises_type_error(self) -> None:
        """A key the schema has no field for is rejected, not silently dropped.

        Deliberate divergence from the Go codec (which drops it): silent
        truncation is never acceptable under invariant 6.
        """
        schema = _schema_with_fields({"name": "id", "type": "long"})
        with pytest.raises(TypeError, match='unknown field "extra" in record "R"'):
            schema.encode({"id": 1, "extra": "x"})

    def test_missing_required_field_raises_value_error(self) -> None:
        schema = _schema_with_fields({"name": "id", "type": "long"})
        with pytest.raises(ValueError, match='missing required field "id" in record "R"'):
            schema.encode({})

    def test_missing_field_with_default_is_allowed_and_writes_the_default(self) -> None:
        """Both codecs write a field's default when the value omits it (verified vs Go)."""
        schema = _schema_with_fields(
            {"name": "id", "type": "long"},
            {"name": "note", "type": ["null", "string"], "default": None},
        )
        encoded = schema.encode({"id": 1})
        assert schema.decode(encoded) == {"id": 1, "note": None}

    def test_out_of_range_int_raises_overflow_error(self) -> None:
        schema = _schema_with_fields({"name": "i", "type": "int"})
        with pytest.raises(OverflowError, match="out of range for Avro int"):
            schema.encode({"i": 2**40})

    def test_union_branch_no_match_raises_type_error(self) -> None:
        schema = _schema_with_fields({"name": "u", "type": ["null", "string"]})
        with pytest.raises(TypeError, match="matches no branch of union"):
            schema.encode({"u": 42})

    def test_map_value_type_mismatch_raises_type_error(self) -> None:
        schema = _schema_with_fields({"name": "attrs", "type": {"type": "map", "values": "string"}})
        with pytest.raises(TypeError, match=r'field "attrs<k>".*unsupported for Avro string'):
            schema.encode({"attrs": {"k": 42}})

    def test_map_with_non_string_key_raises_type_error(self) -> None:
        schema = _schema_with_fields({"name": "attrs", "type": {"type": "map", "values": "string"}})
        with pytest.raises(TypeError, match="map keys must be strings"):
            schema.encode({"attrs": {1: "x"}})

    def test_nested_record_error_names_the_full_path(self) -> None:
        schema = AvroSchema(
            json.dumps(
                {
                    "type": "record",
                    "name": "Outer",
                    "fields": [
                        {
                            "name": "inner",
                            "type": {
                                "type": "record",
                                "name": "Inner",
                                "fields": [{"name": "x", "type": "long"}],
                            },
                        }
                    ],
                }
            )
        )
        with pytest.raises(TypeError, match=r'field "inner.x".*unsupported for Avro long'):
            schema.encode({"inner": {"x": True}})

    def test_encode_and_decode_of_a_non_record_schema_raise_type_error(self) -> None:
        """AvroSchema is documented as record-shaped (dict-in/dict-out) only."""
        schema = AvroSchema(json.dumps("long"))
        with pytest.raises(TypeError, match="not an Avro record"):
            schema.encode({"a": 1})  # type: ignore[arg-type]  -- not a record schema
        with pytest.raises(TypeError, match="did not decode to a record"):
            schema.decode(b"\x00")


class TestAvroSchemaErrors:
    """Parsing failures are one predictable exception (ValueError), and encode/
    decode failures are explicit errors, never silent coercion (invariant 6)."""

    def test_invalid_json_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="invalid Avro schema"):
            AvroSchema("not json")

    def test_invalid_schema_shape_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="invalid Avro schema"):
            AvroSchema(json.dumps({"type": "not-a-real-avro-type"}))

    def test_schema_without_a_type_raises_value_error(self) -> None:
        """fastavro raises a raw KeyError for this; the SDK normalizes it to ValueError."""
        with pytest.raises(ValueError, match="invalid Avro schema"):
            AvroSchema(json.dumps({"fields": []}))

    def test_schema_with_string_fields_raises_value_error(self) -> None:
        """fastavro raises a raw AttributeError for this; normalized to ValueError."""
        with pytest.raises(ValueError, match="invalid Avro schema"):
            AvroSchema(json.dumps({"type": "record", "name": "X", "fields": "nope"}))

    def test_schema_with_a_list_type_raises_value_error(self) -> None:
        """fastavro raises a raw TypeError for this; normalized to ValueError."""
        with pytest.raises(ValueError, match="invalid Avro schema"):
            AvroSchema(json.dumps({"type": ["null"]}))

    def test_parse_classmethod_is_equivalent_to_constructor(self) -> None:
        text = json.dumps(
            {"type": "record", "name": "X", "fields": [{"name": "a", "type": "long"}]}
        )
        via_classmethod = AvroSchema.parse(text)
        via_constructor = AvroSchema(text)
        value = {"a": 1}
        assert via_classmethod.encode(value) == via_constructor.encode(value)

    def test_text_property_returns_original_schema_text(self) -> None:
        text = json.dumps(
            {"type": "record", "name": "X", "fields": [{"name": "a", "type": "long"}]}
        )
        assert AvroSchema(text).text == text

    def test_encode_of_a_wrong_type_field_raises_explicitly(self) -> None:
        """A string where the schema says long is a loud error, not silent coercion."""
        value = _encode_value(_GOLDEN["cases"][0]) | {"id": "not-an-int"}
        with pytest.raises((TypeError, ValueError, OverflowError)):
            _schema_for(_GOLDEN["cases"][0]).encode(value)

    def test_decode_of_truncated_bytes_raises_explicitly(self) -> None:
        case = next(c for c in _GOLDEN["cases"] if c.get("go_avro_hex"))
        golden_bytes = bytes.fromhex(case["go_avro_hex"])
        # fastavro: EOFError for truncation; ValueError/IndexError for other malformed input.
        with pytest.raises((EOFError, ValueError, IndexError)):
            _schema_for(case).decode(golden_bytes[: len(golden_bytes) // 2])

    def test_decode_of_empty_bytes_raises_explicitly(self) -> None:
        with pytest.raises((EOFError, ValueError)):
            _schema_for(_GOLDEN["cases"][0]).decode(b"")

    def test_missing_fastavro_raises_module_not_found_with_install_hint(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Without the [avro] extra, the failure names the fix instead of a bare import error."""
        monkeypatch.setattr(schema_module, "_fastavro_module", None)
        real_import = importlib.import_module

        def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
            if name == "fastavro":
                raise ModuleNotFoundError("No module named 'fastavro'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(schema_module.importlib, "import_module", fake_import)
        with pytest.raises(ModuleNotFoundError, match=r"\[avro\]"):
            schema_module._fastavro()  # direct check of the lazy loader
