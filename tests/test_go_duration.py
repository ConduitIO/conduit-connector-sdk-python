"""Tests for :func:`conduit.config.format_go_duration`/``parse_go_duration``.

The Go-duration mapping (design doc §2.2's Duration A-gap, now closed):
``config.Parameter.Type.TYPE_DURATION`` fields serialize/parse using Go's
``time.Duration.String()``/``ParseDuration`` syntax (``"5s"``, ``"1h30m"``,
``"500ms"``), not ISO-8601. These tests cover known Go-format examples,
round-trip identity (including via Hypothesis over arbitrary microsecond
counts), and malformed-input rejection.
"""

from __future__ import annotations

import datetime

import pytest
from hypothesis import given
from hypothesis import strategies as st

from conduit.config import format_go_duration, parse_go_duration

# Known-good (string, timedelta) pairs matching Go's actual `String()` output
# for the same duration -- these pin compatibility with real Go tooling, not
# just internal self-consistency.
_KNOWN_GO_FORMATS = [
    (datetime.timedelta(0), "0s"),
    (datetime.timedelta(microseconds=500), "500µs"),
    (datetime.timedelta(milliseconds=500), "500ms"),
    (datetime.timedelta(microseconds=1500), "1.5ms"),
    (datetime.timedelta(milliseconds=1500), "1.5s"),
    (datetime.timedelta(seconds=1), "1s"),
    (datetime.timedelta(seconds=45), "45s"),
    (datetime.timedelta(seconds=90), "1m30s"),
    (datetime.timedelta(minutes=1), "1m0s"),
    (datetime.timedelta(hours=1), "1h0m0s"),
    (datetime.timedelta(hours=1, minutes=30), "1h30m0s"),
    (datetime.timedelta(hours=2, minutes=45, seconds=30), "2h45m30s"),
    (-datetime.timedelta(seconds=5), "-5s"),
]


@pytest.mark.parametrize(("value", "expected"), _KNOWN_GO_FORMATS)
def test_format_matches_known_go_output(value: datetime.timedelta, expected: str) -> None:
    assert format_go_duration(value) == expected


@pytest.mark.parametrize(("value", "expected"), _KNOWN_GO_FORMATS)
def test_format_then_parse_round_trips(value: datetime.timedelta, expected: str) -> None:
    assert parse_go_duration(format_go_duration(value)) == value


class TestParseAcceptsGoSyntaxVariants:
    def test_plain_seconds(self) -> None:
        assert parse_go_duration("5s") == datetime.timedelta(seconds=5)

    def test_combined_hours_minutes(self) -> None:
        assert parse_go_duration("1h30m") == datetime.timedelta(hours=1, minutes=30)

    def test_milliseconds(self) -> None:
        assert parse_go_duration("500ms") == datetime.timedelta(milliseconds=500)

    def test_fractional_hours(self) -> None:
        assert parse_go_duration("1.5h") == datetime.timedelta(hours=1.5)

    def test_combined_hours_minutes_seconds(self) -> None:
        assert parse_go_duration("2h45m30s") == datetime.timedelta(hours=2, minutes=45, seconds=30)

    def test_negative(self) -> None:
        assert parse_go_duration("-1.5h") == -datetime.timedelta(hours=1.5)

    def test_explicit_plus_sign(self) -> None:
        assert parse_go_duration("+5s") == datetime.timedelta(seconds=5)

    def test_microseconds_ascii_spelling(self) -> None:
        assert parse_go_duration("500us") == datetime.timedelta(microseconds=500)

    def test_microseconds_mu_spelling(self) -> None:
        assert parse_go_duration("500µs") == datetime.timedelta(microseconds=500)

    def test_bare_zero_with_no_unit(self) -> None:
        assert parse_go_duration("0") == datetime.timedelta(0)

    def test_surrounding_whitespace_is_tolerated(self) -> None:
        assert parse_go_duration("  5s  ") == datetime.timedelta(seconds=5)


class TestParseRejectsInvalidSyntax:
    def test_empty_string(self) -> None:
        with pytest.raises(ValueError, match="empty"):
            parse_go_duration("")

    def test_number_with_no_unit(self) -> None:
        with pytest.raises(ValueError, match="Go duration syntax"):
            parse_go_duration("5")

    def test_unit_with_no_number(self) -> None:
        with pytest.raises(ValueError, match="Go duration syntax"):
            parse_go_duration("s")

    def test_unknown_unit(self) -> None:
        with pytest.raises(ValueError, match="Go duration syntax"):
            parse_go_duration("5x")

    def test_garbage(self) -> None:
        with pytest.raises(ValueError, match="Go duration syntax"):
            parse_go_duration("not-a-duration")

    def test_gap_between_components(self) -> None:
        with pytest.raises(ValueError, match="unexpected characters"):
            parse_go_duration("5s garbage 3m")


@given(
    total_us=st.integers(
        min_value=-int(1e12),  # roughly -11.5 days, comfortably within timedelta range
        max_value=int(1e12),
    )
)
def test_format_parse_round_trip_property(total_us: int) -> None:
    """For any representable microsecond count, format -> parse recovers it exactly."""
    value = datetime.timedelta(microseconds=total_us)
    formatted = format_go_duration(value)
    assert parse_go_duration(formatted) == value
