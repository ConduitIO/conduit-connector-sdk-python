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

See `conduit/schema.py`'s module docstring for the full contract and its
two documented, honest limits (array block-framing style, not byte-identical
in one direction for non-empty arrays -- still mutually decodable; map
fields nondeterministic on the Go side by construction, hence absent from
the fixture).
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

from conduit.record import Metadata
from conduit.schema import AvroSchema

_GOLDEN_PATH = Path(__file__).parent / "testdata" / "avro_golden.json"
_GOLDEN: dict[str, Any] = json.loads(_GOLDEN_PATH.read_text())


@pytest.fixture(scope="module")
def schema() -> AvroSchema:
    return AvroSchema(_GOLDEN["schema_json"])


@pytest.mark.parametrize("case", _GOLDEN["cases"], ids=lambda c: c["name"])
class TestGoProducedBytesDecodeCorrectlyInPython:
    """The direction that matters most: can this SDK read what Go wrote?

    Every case is byte-exact-verified for this direction implicitly (the
    golden bytes decode to exactly the expected value), regardless of the
    array-framing caveat -- decoding handles both framing styles per the
    Avro spec, which is exactly why this direction has no exceptions.
    """

    def test_decodes_to_the_expected_value(self, schema: AvroSchema, case: dict[str, Any]) -> None:
        golden_bytes = bytes.fromhex(case["go_avro_hex"])
        decoded = schema.decode(golden_bytes)
        assert decoded == case["value"]


@pytest.mark.parametrize("case", _GOLDEN["cases"], ids=lambda c: c["name"])
class TestPythonEncodingIsPinnedToGoVerifiedBytes:
    """Python's encode output is pinned to bytes Go has verified it can read.

    ``python_avro_hex`` was produced by this SDK's own encoder and verified
    by `tools/avro_fixture_gen` (Go decodes it to the expected value). Pinning
    it here makes the Python side of the cross-language proof run in
    Python-only CI -- any change to the encoder that alters the bytes (or to
    the pinned values) fails loudly instead of drifting.
    """

    def test_encode_matches_committed_go_verified_bytes_exactly(
        self, schema: AvroSchema, case: dict[str, Any]
    ) -> None:
        encoded = schema.encode(case["value"])
        assert encoded == bytes.fromhex(case["python_avro_hex"]), (
            "encode() output drifted from the Go-verified python_avro_hex; "
            "re-verify with tools/avro_fixture_gen before committing new bytes"
        )

    def test_round_trips_through_this_sdk(self, schema: AvroSchema, case: dict[str, Any]) -> None:
        encoded = schema.encode(case["value"])
        assert schema.decode(encoded) == case["value"]


@pytest.mark.parametrize(
    "case",
    [c for c in _GOLDEN["cases"] if c["byte_exact_encode_both_directions"]],
    ids=lambda c: c["name"],
)
def test_byte_exact_bidirectional_cases_match_go_exactly(
    schema: AvroSchema, case: dict[str, Any]
) -> None:
    """For cases without array framing ambiguity, bytes match Go exactly, not just values."""
    golden_bytes = bytes.fromhex(case["go_avro_hex"])
    encoded = schema.encode(case["value"])
    assert encoded == golden_bytes, (
        "expected byte-exact match with Go's iskorotkov/avro output for a case "
        "explicitly marked unambiguous (no non-empty array field encoding choice)"
    )


def test_array_field_case_is_decode_compatible_but_not_byte_identical_on_encode(
    schema: AvroSchema,
) -> None:
    """Documents (and pins) the one real, honest divergence -- see the module docstring.

    If this ever starts passing (bytes become identical), the encoders
    converged and the ``byte_exact_encode_both_directions: false`` case in
    the golden fixture -- and this test -- should be updated, not silently
    left stale.
    """
    case = next(c for c in _GOLDEN["cases"] if not c["byte_exact_encode_both_directions"])
    golden_bytes = bytes.fromhex(case["go_avro_hex"])
    encoded = schema.encode(case["value"])
    assert encoded != golden_bytes, (
        "expected the documented array-framing byte difference; if this now "
        "matches, update the golden fixture's byte_exact_encode_both_directions flag"
    )
    # But both still decode to the identical value -- that's the property
    # that actually matters (see module docstring).
    assert schema.decode(golden_bytes) == schema.decode(encoded) == case["value"]


@pytest.mark.timeout(180)  # first `go run` downloads the module cache; overrides the 60s global
def test_go_verifier_confirms_python_bytes_decode_in_go() -> None:
    """Re-run the Go verifier live: Go decodes this SDK's pinned bytes.

    ``tools/avro_fixture_gen`` re-derives ``go_avro_hex`` from the committed
    values and decodes ``python_avro_hex`` (this SDK's encode output) back
    to the expected values, using the exact Avro library conduit-commons
    wraps. Skipped when no Go toolchain is present (the committed fixture
    plus the pinned-bytes tests above carry the proof in Python-only CI);
    when Go exists, a failure here is a real cross-language regression.
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


class TestSchemaRegistrationAndMetadataContract:
    """The author-facing "registration" flow the design doc §2.6 specifies.

    No schema registry client exists in this SDK (documented scope
    boundary): "registration" is the author supplying schema text, encoding
    with :class:`AvroSchema`, and recording the schema reference as
    ``opencdc.{key,payload}.schema.{subject,version}`` metadata via
    :class:`conduit.record.Metadata`. These tests pin that contract,
    including that the reference lives in metadata, not in the bytes.
    """

    def test_register_encode_and_attach_metadata_end_to_end(self, schema: AvroSchema) -> None:
        """The full author flow: parse, encode a record, attach subject/version."""
        value = _GOLDEN["cases"][1]["value"]
        metadata: dict[str, str] = {}
        Metadata.set_payload_schema(
            metadata, subject="io.conduit.example.OpenCDCPayload", version=1
        )

        encoded = schema.encode(value)

        # The bytes are exactly what Go produces/accepts for this value --
        # and the schema reference that lets a downstream consumer decode
        # them is in the metadata, not embedded in the bytes.
        assert Metadata.get_payload_schema(metadata) == (
            "io.conduit.example.OpenCDCPayload",
            1,
        )
        assert schema.decode(encoded) == value

    def test_the_schema_reference_is_not_embedded_in_the_bytes(self, schema: AvroSchema) -> None:
        """Same value + same schema, different subject/version -> identical bytes.

        This is the design doc's schema-id behavior stated as a property:
        the plain (headerless) Avro wire format has no schema-ID header, so
        changing the metadata reference must not change the encoding --
        there is nothing in the bytes for it to change.
        """
        value = _GOLDEN["cases"][1]["value"]
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


class TestAvroSchemaErrors:
    """Encoding/decoding failures are explicit errors, never silent coercion (invariant 6)."""

    def test_invalid_json_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="invalid Avro schema"):
            AvroSchema("not json")

    def test_invalid_schema_shape_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="invalid Avro schema"):
            AvroSchema(json.dumps({"type": "not-a-real-avro-type"}))

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

    def test_decode_of_a_non_record_schema_raises_type_error(self) -> None:
        """AvroSchema is documented as record-shaped (dict-in/dict-out) only."""
        schema = AvroSchema(json.dumps("long"))
        encoded = schema.encode(5)  # type: ignore[arg-type]  -- deliberately not a Mapping
        with pytest.raises(TypeError, match="did not decode to a record"):
            schema.decode(encoded)

    def test_encode_of_a_wrong_type_field_raises_explicitly(self, schema: AvroSchema) -> None:
        """A string where the schema says long is a loud error, not silent coercion."""
        value = _GOLDEN["cases"][1]["value"] | {"id": "not-an-int"}
        with pytest.raises((TypeError, ValueError, OverflowError)):
            schema.encode(value)

    def test_encode_of_a_missing_required_field_raises_explicitly(self, schema: AvroSchema) -> None:
        """A record missing a required field is a loud error, not a partial write."""
        with pytest.raises((TypeError, ValueError, OverflowError)):
            schema.encode({"id": 1})

    def test_decode_of_truncated_bytes_raises_explicitly(self, schema: AvroSchema) -> None:
        golden_bytes = bytes.fromhex(_GOLDEN["cases"][0]["go_avro_hex"])
        # fastavro: EOFError for truncation; ValueError/IndexError for other malformed input.
        with pytest.raises((EOFError, ValueError, IndexError)):
            schema.decode(golden_bytes[: len(golden_bytes) // 2])

    def test_decode_of_empty_bytes_raises_explicitly(self, schema: AvroSchema) -> None:
        with pytest.raises((EOFError, ValueError)):
            schema.decode(b"")
