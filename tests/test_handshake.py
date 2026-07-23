"""Unit tests for :mod:`conduit._handshake`.

These assert the exact wire format go-plugin's client parser expects
(``go-plugin@v1.8.0/client.go:838-926``: split on ``|``, at least 4 parts),
per the design doc's Risks & open questions #1 -- a hand-rolled harness that
doesn't check against the real parser's assumptions could pass while the
real client rejects the line. Full end-to-end confirmation additionally
requires a real Conduit launch (compat-nightly.yml, not this file).
"""

from __future__ import annotations

import io

import pytest

from conduit._handshake import (
    CORE_PROTOCOL_VERSION,
    MAGIC_COOKIE_KEY,
    MAGIC_COOKIE_VALUE,
    HandshakeError,
    HandshakeLine,
    check_magic_cookie,
    emit_handshake_line,
    negotiate_protocol_version,
)


class TestCheckMagicCookie:
    def test_valid_cookie_passes(self) -> None:
        check_magic_cookie({MAGIC_COOKIE_KEY: MAGIC_COOKIE_VALUE})

    def test_missing_cookie_raises(self) -> None:
        with pytest.raises(HandshakeError, match=MAGIC_COOKIE_KEY):
            check_magic_cookie({})

    def test_wrong_cookie_raises(self) -> None:
        with pytest.raises(HandshakeError, match=MAGIC_COOKIE_KEY):
            check_magic_cookie({MAGIC_COOKIE_KEY: "not-the-real-cookie"})

    def test_empty_string_cookie_raises(self) -> None:
        with pytest.raises(HandshakeError):
            check_magic_cookie({MAGIC_COOKIE_KEY: ""})


class TestNegotiateProtocolVersion:
    def test_picks_v2_when_client_supports_v1_and_v2(self) -> None:
        assert negotiate_protocol_version({"PLUGIN_PROTOCOL_VERSIONS": "1,2"}) == 2

    def test_picks_v2_when_only_v2_advertised(self) -> None:
        assert negotiate_protocol_version({"PLUGIN_PROTOCOL_VERSIONS": "2"}) == 2

    def test_tolerates_whitespace_around_versions(self) -> None:
        assert negotiate_protocol_version({"PLUGIN_PROTOCOL_VERSIONS": " 1, 2 "}) == 2

    def test_missing_env_var_raises(self) -> None:
        with pytest.raises(HandshakeError, match="PLUGIN_PROTOCOL_VERSIONS"):
            negotiate_protocol_version({})

    def test_empty_env_var_raises(self) -> None:
        with pytest.raises(HandshakeError, match="PLUGIN_PROTOCOL_VERSIONS"):
            negotiate_protocol_version({"PLUGIN_PROTOCOL_VERSIONS": ""})

    def test_no_overlap_raises(self) -> None:
        # Client only speaks the deprecated v1 protocol; this SDK doesn't.
        with pytest.raises(HandshakeError, match="no protocol version in common"):
            negotiate_protocol_version({"PLUGIN_PROTOCOL_VERSIONS": "1"})

    def test_malformed_value_raises(self) -> None:
        with pytest.raises(HandshakeError, match="not a comma-separated list"):
            negotiate_protocol_version({"PLUGIN_PROTOCOL_VERSIONS": "two"})

    def test_future_protocol_version_still_negotiates_down_to_v2(self) -> None:
        # Per the design doc's Upgrade/rollback section: an unmodified v2-only
        # SDK must keep working if Conduit's client one day also advertises a
        # hypothetical v3, as long as v2 stays in the client's version list.
        assert negotiate_protocol_version({"PLUGIN_PROTOCOL_VERSIONS": "2,3"}) == 2


class TestHandshakeLineFormat:
    def test_field_order_and_count(self) -> None:
        line = HandshakeLine(app_protocol_version=2, address="127.0.0.1:54321")
        parts = line.format().split("|")
        # go-plugin's client parser requires at least 4 parts; this SDK always
        # emits all 6 documented fields.
        assert len(parts) == 6
        assert parts == [
            str(CORE_PROTOCOL_VERSION),
            "2",
            "tcp",
            "127.0.0.1:54321",
            "grpc",
            "",  # server_cert, always empty -- no AutoMTLS (see class docstring)
        ]

    def test_core_protocol_version_is_always_1(self) -> None:
        line = HandshakeLine(app_protocol_version=2, address="127.0.0.1:1")
        assert line.format().split("|")[0] == "1"

    def test_protocol_is_always_grpc(self) -> None:
        line = HandshakeLine(app_protocol_version=2, address="127.0.0.1:1")
        assert line.format().split("|")[4] == "grpc"

    def test_server_cert_field_present_but_empty(self) -> None:
        # A trailing empty field still yields a trailing "|" -- the parser
        # requires >= 4 parts, and split("|") on a trailing delimiter
        # produces an explicit empty string, not a dropped field.
        line = HandshakeLine(app_protocol_version=2, address="127.0.0.1:1")
        formatted = line.format()
        assert formatted.endswith("|")
        assert formatted.split("|")[-1] == ""


class TestEmitHandshakeLine:
    def test_writes_single_newline_terminated_line(self) -> None:
        buf = io.StringIO()
        line = HandshakeLine(app_protocol_version=2, address="127.0.0.1:9")
        emit_handshake_line(line, stream=buf)
        output = buf.getvalue()
        assert output == line.format() + "\n"
        assert output.count("\n") == 1

    def test_flushes_the_stream(self) -> None:
        flushed = False

        class TrackingStream(io.StringIO):
            def flush(self) -> None:
                nonlocal flushed
                flushed = True
                super().flush()

        stream = TrackingStream()
        emit_handshake_line(HandshakeLine(app_protocol_version=2, address="x:1"), stream=stream)
        assert flushed is True
