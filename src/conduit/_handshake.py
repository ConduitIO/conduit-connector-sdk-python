"""Replicates HashiCorp go-plugin's subprocess handshake, pure-Python side.

Conduit launches a standalone connector as a subprocess and communicates with
it using `go-plugin <https://github.com/hashicorp/go-plugin>`_'s handshake
protocol. There is no shared Go runtime a Python process can lean on, so this
module reimplements the client-facing half of that protocol directly against
constants and line-format semantics cited from go-plugin and
``conduit-connector-protocol`` source, not inferred.

See ``docs/design/20260707-python-connector-sdk.md`` §1 for the full citation
trail (go-plugin ``server.go``/``client.go`` line numbers, protocol version
semantics, and ▶ MUST-FIX 1's re-verification of the related Go SDK
citations). This module is intentionally self-contained and has no gRPC
dependency, so it can be unit-tested against the exact wire format
go-plugin's client parses without standing up a real server -- the design
doc's Risks & open questions #1 names getting this subtly wrong as the
single biggest execution risk for this SDK, precisely because a wrong
handshake fails silently as "Conduit hangs" rather than a clear error.

**Invariant 7 (graceful shutdown) touches this module only indirectly**: the
handshake itself has no shutdown behavior, but a malformed or missing
handshake line means Conduit's dispenser can never establish the connection
it would otherwise use to send ``GRPCController.Shutdown`` -- see
``serve.py`` (Lane B, not implemented in this module) for where that RPC is
handled.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TextIO

# --- Magic cookie -------------------------------------------------------
#
# Conduit and the Go SDK share the literal `pconnector.HandshakeConfig` value
# (conduit-connector-protocol/pconnector/pconnector.go:19-22); there is no
# per-language derivation of this value, it must be copied verbatim.
MAGIC_COOKIE_KEY = "CONDUIT_PLUGIN_MAGIC_COOKIE"
MAGIC_COOKIE_VALUE = "204e8e812c3a1bb73b838928c575b42a105dd2e9aa449be481bc4590486df53f"

# go-plugin's own "core" protocol version (server.go:33) -- unrelated to the
# Conduit connector-protocol (pconnector) version negotiated below. This is
# always 1 today; go-plugin has never bumped it.
CORE_PROTOCOL_VERSION = 1

# Conduit connector protocol (pconnector) app-protocol versions this SDK
# implements. v1 is deprecated at the source
# (conduit-connector-protocol/pconnector/v1/version.go, doc-commented
# "Deprecated: v1 is deprecated. Use v2 instead."); this SDK targets v2 only,
# per docs/design/20260707-python-connector-sdk.md §1.1.
SUPPORTED_APP_PROTOCOL_VERSIONS: tuple[int, ...] = (2,)

# go-plugin client env var advertising which app-protocol versions the host
# (Conduit) supports (go-plugin@v1.8.0/client.go:642).
PROTOCOL_VERSIONS_ENV = "PLUGIN_PROTOCOL_VERSIONS"


class HandshakeError(RuntimeError):
    """The go-plugin handshake could not be completed.

    Any raise of this must be caught by the caller and reported on **stderr**
    before process exit -- never on stdout. Stdout is the single channel
    go-plugin's client parses; anything printed there that isn't the
    handshake line itself corrupts the channel (see this module's docstring
    and the design doc's Failure modes section).
    """


def check_magic_cookie(env: Mapping[str, str] | None = None) -> None:
    """Validate the magic-cookie env var set by go-plugin's client.

    Mirrors go-plugin's own server-side check
    (``go-plugin@v1.8.0/server.go:247-266``). Conduit's client does not
    validate this independently on its side beyond setting the env var, so a
    Python plugin must perform this check itself -- there is no free lunch
    from a shared runtime.

    Args:
        env: environment mapping to read from; defaults to ``os.environ``.
            Injectable for testing.

    Raises:
        HandshakeError: if the cookie is missing or does not match exactly.
    """
    env = env if env is not None else os.environ
    value = env.get(MAGIC_COOKIE_KEY)
    if value != MAGIC_COOKIE_VALUE:
        raise HandshakeError(
            f"{MAGIC_COOKIE_KEY} is missing or does not match the expected "
            "value. This binary must be launched by Conduit (or a compatible "
            "go-plugin host) as a plugin subprocess -- it is not meant to be "
            "run directly."
        )


def negotiate_protocol_version(
    env: Mapping[str, str] | None = None,
    supported: Sequence[int] = SUPPORTED_APP_PROTOCOL_VERSIONS,
) -> int:
    """Pick the highest app-protocol version both sides support.

    Reads ``PLUGIN_PROTOCOL_VERSIONS`` (a comma-separated list of ints) set by
    go-plugin's client (``go-plugin@v1.8.0/client.go:642``) and intersects it
    with ``supported``, returning the highest common version.

    A client that does not advertise ``PLUGIN_PROTOCOL_VERSIONS`` at all is a
    legacy go-plugin client that implicitly expects app-protocol version 1.
    This SDK does not implement v1 (deprecated at the protocol source, see
    module docstring), so that case is a hard error rather than a silent v1
    fallback that would then fail differently and more confusingly later.

    Args:
        env: environment mapping to read from; defaults to ``os.environ``.
        supported: app-protocol versions this SDK build implements, highest
            first is not required -- the function sorts.

    Returns:
        The negotiated app-protocol version (currently always ``2``).

    Raises:
        HandshakeError: if the env var is absent/empty, malformed, or has no
            version in common with ``supported``.
    """
    env = env if env is not None else os.environ
    raw = env.get(PROTOCOL_VERSIONS_ENV, "")
    if not raw.strip():
        raise HandshakeError(
            f"{PROTOCOL_VERSIONS_ENV} is not set or empty -- cannot negotiate "
            f"a protocol version. This SDK supports version(s) {list(supported)!r} "
            "only; a client that omits this variable is implicitly requesting "
            "the deprecated v1 protocol, which this SDK does not implement."
        )

    try:
        client_versions = {int(v) for v in raw.split(",") if v.strip()}
    except ValueError as e:
        raise HandshakeError(
            f"{PROTOCOL_VERSIONS_ENV}={raw!r} is not a comma-separated list of integers"
        ) from e

    overlap = sorted(set(supported) & client_versions, reverse=True)
    if not overlap:
        raise HandshakeError(
            f"no protocol version in common: client supports "
            f"{sorted(client_versions)}, this SDK supports {sorted(supported)}"
        )
    return overlap[0]


@dataclass(frozen=True, slots=True)
class HandshakeLine:
    """The single line this process prints to stdout once its gRPC server is listening.

    Field order and semantics are cited to
    ``go-plugin@v1.8.0/server.go:426-445`` (writer side) and
    ``go-plugin@v1.8.0/client.go:838-926`` (parser side, splits on ``|``,
    requires at least 4 parts). Per
    ``docs/design/20260707-python-connector-sdk.md`` §1.1.4, this SDK always
    uses TCP, never Unix domain sockets (the parser accepts either, but TCP
    sidesteps Unix-socket temp-directory/permission handling and gets Windows
    support without a platform branch).
    """

    app_protocol_version: int
    """The negotiated Conduit connector-protocol version (from
    :func:`negotiate_protocol_version`) -- currently always ``2``."""

    address: str
    """The listen address, e.g. ``127.0.0.1:54321`` for the TCP transport
    this SDK always uses."""

    network: str = "tcp"
    """Always ``"tcp"`` for this SDK (see class docstring); the field exists
    because the wire format carries it and the parser is transport-agnostic."""

    server_cert: str = ""
    """AutoMTLS certificate, only interpreted by the client if longer than 50
    characters. Conduit's client never sets ``AutoMTLS: true``
    (``pconnector/client/client.go`` has no such option), so this is always
    empty -- no TLS is in play for this SDK."""

    core_protocol_version: int = CORE_PROTOCOL_VERSION
    """go-plugin's own protocol version, always ``1`` -- unrelated to
    ``app_protocol_version``. Do not confuse the two; see module docstring."""

    protocol: str = "grpc"
    """Always ``"grpc"``; go-plugin's legacy NetRPC transport is explicitly
    disabled on both sides (``plugin.NetRPCUnsupportedPlugin``)."""

    def format(self) -> str:
        """Render the exact pipe-delimited line go-plugin's client parses."""
        fields = (
            self.core_protocol_version,
            self.app_protocol_version,
            self.network,
            self.address,
            self.protocol,
            self.server_cert,
        )
        return "|".join(str(f) for f in fields)


def emit_handshake_line(line: HandshakeLine, stream: TextIO | None = None) -> None:
    """Print the handshake line and flush immediately.

    Stdout is a structured channel go-plugin's client parses byte-for-byte;
    nothing else may ever be written to it (see this module's docstring and
    ``docs/design/20260707-python-connector-sdk.md``'s Failure modes section:
    a stray ``print()`` before this line corrupts the handshake). Callers
    must route all logging to stderr instead, before and after calling this.

    Args:
        line: the handshake line to emit.
        stream: stream to write to; defaults to ``sys.stdout``. Injectable
            for testing so tests never depend on real stdout capture timing.
    """
    stream = stream if stream is not None else sys.stdout
    stream.write(line.format() + "\n")
    stream.flush()
