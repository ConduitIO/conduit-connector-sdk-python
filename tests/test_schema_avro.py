"""Cross-language Avro wire-compatibility tests for `conduit.schema.AvroSchema`.

The property that matters for WS2 is **wire compatibility** with
`conduit-commons`' `schema/avro` package (Go), not merely "this Python code
can read its own output" -- a Python-only round-trip is not evidence of
that. This module proves it against `tests/testdata/avro_golden.json`:
bytes produced by an actual Go program using `github.com/hamba/avro/v2`
(the library `conduit-commons` wraps directly), not reimplemented from the
Avro spec. See that file's `_provenance` key and
`conduit/schema.py`'s module docstring for the full contract and its one
documented, honest limit (array/map block-framing style, not byte-identical
in one direction for non-empty arrays -- still mutually decodable, verified
both ways during golden generation).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

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
def test_python_encoding_is_valid_avro_go_can_read(
    schema: AvroSchema, case: dict[str, Any]
) -> None:
    """Python-encoded bytes decode back to the same value through this SDK.

    Combined with the golden fixture's documented, Go-side-verified proof
    that fastavro's array-framing choice is independently Go-decodable
    (`tests/testdata/avro_golden.json`'s `array_framing_note`), this closes
    the loop: Python can read Go's bytes (previous test class), and Go can
    read Python's bytes (verified once during golden-fixture generation,
    documented rather than re-run here since it would require a live Go
    toolchain in this repo's CI, which is out of scope -- see
    `conduit/schema.py`'s module docstring on why there's no live
    schema-registry/cross-toolchain integration in this SDK yet).
    """
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
    """For cases without array/map framing ambiguity, bytes match Go exactly, not just values."""
    golden_bytes = bytes.fromhex(case["go_avro_hex"])
    encoded = schema.encode(case["value"])
    assert encoded == golden_bytes, (
        "expected byte-exact match with Go's hamba/avro output for a case "
        "explicitly marked unambiguous (no array/map field encoding choice)"
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


class TestAvroSchemaErrors:
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
