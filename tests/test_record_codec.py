"""Hypothesis round-trip tests for the OpenCDC record (de)serialization codec.

Covers design doc §2.3/B3: raw ``bytes`` payloads must round-trip through
``record_to_proto``/``record_from_proto`` with **exact** identity (not
`5 == 5.0`-style laxity). Structured (``dict``) payloads cross
``google.protobuf.Struct``, which represents every JSON-like value
(including integers) as a double -- this file both proves the common case
round-trips and explicitly demonstrates (pins, not merely tolerates) the
known int->float precision-loss case for large integers, per the design
doc's requirement that a test accepting `5 == 5.0` would wrongly pass while
masking that case.
"""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from conduit._grpc.adapters import record_from_proto, record_to_proto
from conduit.record import Change, Operation, Record

# JSON-like scalar/collection strategy for `structured_data`, deliberately
# bounded to integers that fit in a double's 53-bit mantissa exactly -- this
# is the "faithful round-trip" side of B3; the precision-loss side is tested
# separately and explicitly below, not folded into this "should round-trip
# exactly" strategy.
_EXACT_JSON_INT = st.integers(min_value=-(2**53), max_value=2**53)
_JSON_SCALAR = st.one_of(
    st.none(),
    st.booleans(),
    _EXACT_JSON_INT,
    st.floats(allow_nan=False, allow_infinity=False, width=32),
    st.text(max_size=20),
)
_JSON_VALUE = st.recursive(
    _JSON_SCALAR,
    lambda children: st.one_of(
        st.lists(children, max_size=3),
        st.dictionaries(st.text(max_size=10), children, max_size=3),
    ),
    max_leaves=6,
)
_STRUCTURED_DATA = st.dictionaries(st.text(min_size=1, max_size=10), _JSON_VALUE, max_size=5)

_OPERATIONS = st.sampled_from(list(Operation))
_METADATA = st.dictionaries(st.text(max_size=10), st.text(max_size=10), max_size=5)


@given(
    position=st.binary(max_size=32),
    operation=_OPERATIONS,
    metadata=_METADATA,
    key=st.binary(max_size=32),
    before=st.one_of(st.none(), st.binary(max_size=32)),
    after=st.one_of(st.none(), st.binary(max_size=32)),
)
def test_raw_bytes_round_trip_is_exact(
    position: bytes,
    operation: Operation,
    metadata: dict[str, str],
    key: bytes,
    before: bytes | None,
    after: bytes | None,
) -> None:
    """Raw ``bytes`` payloads round-trip through proto with exact identity.

    Asserts real equality (``==``) on ``bytes`` objects -- there is no
    "close enough" for raw bytes, unlike the structured-data/int case below.
    """
    record = Record(
        position=position,
        operation=operation,
        metadata=metadata,
        key=key,
        payload=Change(before=before, after=after),
    )

    decoded = record_from_proto(record_to_proto(record))

    assert decoded.position == position
    assert decoded.operation == operation
    assert decoded.metadata == metadata
    assert decoded.key == key
    assert isinstance(decoded.key, bytes)
    assert decoded.payload.before == before
    assert decoded.payload.after == after


@given(key=_STRUCTURED_DATA, after=_STRUCTURED_DATA)
def test_structured_data_with_exact_ints_round_trips(
    key: dict[str, object], after: dict[str, object]
) -> None:
    """Structured (``dict``) payloads round-trip when integers fit a double exactly.

    Bounded to ``abs(value) <= 2**53`` (:data:`_EXACT_JSON_INT`) -- within
    that range, ``google.protobuf.Struct``'s double-precision
    ``number_value`` representation loses no information, so decoding
    yields back a `float` that compares equal to the original `int` via
    Python's numeric tower (``5 == 5.0``). This test is intentionally
    narrower than "any Python dict" -- see
    :func:`test_large_int_in_structured_data_loses_precision_silently` for
    the case this strategy is bounded specifically to exclude.
    """
    record = Record(
        position=b"pos",
        operation=Operation.CREATE,
        key=key,
        payload=Change(after=after),
    )

    decoded = record_from_proto(record_to_proto(record))

    assert decoded.key == key
    assert decoded.payload.after == after


def test_large_int_in_structured_data_loses_precision_silently() -> None:
    """Pin the B3 int->float precision loss for large integers -- silent, not an error.

    ``google.protobuf.Struct`` has no integer type; every JSON-like number
    is a double. ``2**60 + 1`` is not exactly representable as a double, so
    it decodes back as a **different**, nearby integer -- with no exception
    raised anywhere in the encode/decode path. This test exists specifically
    to fail if a future change makes this silently "look like it round-trips"
    (e.g. by accepting `!=` as a bug and papering over it with rounding) --
    the documented contract (design doc §2.3/B3) is that this precision loss
    is known and must remain visible, not hidden.
    """
    large_int = 2**60 + 1  # not exactly representable as an IEEE 754 double
    record = Record(
        position=b"pos",
        operation=Operation.CREATE,
        payload=Change(after={"count": large_int}),
    )

    decoded = record_from_proto(record_to_proto(record))

    assert isinstance(decoded.payload.after, dict)
    decoded_count = decoded.payload.after["count"]
    # The precision loss itself, pinned exactly: silently a `float`, and
    # silently a different numeric value than the original `int`.
    assert isinstance(decoded_count, float)
    assert decoded_count != large_int
    assert decoded_count == float(large_int)  # exactly what a double *can* represent


def test_bytes_inside_structured_data_fails_loudly_not_silently() -> None:
    """Contrast case for B3: bytes (unlike large ints) fail loudly, not silently.

    ``google.protobuf.Struct`` has no representation for raw bytes inside a
    structured value at all -- this raises immediately at encode time,
    unlike the integer case, which succeeds but silently loses precision.
    Documented at the exact conversion site,
    ``conduit._grpc.adapters._data_to_proto``.
    """
    record = Record(
        position=b"pos",
        operation=Operation.CREATE,
        payload=Change(after={"blob": b"not json-representable"}),
    )

    with pytest.raises(ValueError, match="Unexpected type"):
        record_to_proto(record)


def test_empty_raw_data_round_trips_as_empty_bytes_not_none() -> None:
    """An explicitly empty ``key=b""`` decodes back to ``b""``, not ``None``.

    Exercises the ``WhichOneof`` distinction documented in
    ``conduit._grpc.adapters._data_from_proto``: presence of the ``raw_data``
    oneof branch (even when empty) is different from the branch being unset.
    """
    record = Record(position=b"p", operation=Operation.CREATE, key=b"")
    decoded = record_from_proto(record_to_proto(record))
    assert decoded.key == b""
    assert isinstance(decoded.key, bytes)


def test_change_before_and_after_none_round_trip_as_none() -> None:
    """``Change.before``/``after`` left as ``None`` decode back to ``None``, not ``b""``/``{}``.

    This is the "absent" case :func:`conduit._grpc.adapters._data_from_proto_optional`
    exists specifically to distinguish from an explicit empty payload.
    """
    record = Record(position=b"p", operation=Operation.CREATE, payload=Change())
    decoded = record_from_proto(record_to_proto(record))
    assert decoded.payload.before is None
    assert decoded.payload.after is None
