"""Download-on-first-use, version-pinned provisioning of the `conduit` binary.

Per the design doc's failure mode 3 ("Binary/version mismatch between the
client library and the `conduit` engine") and open question 3 ("Binary-
provisioning mechanism for `conduit.local()`"), this module **never** relies
on a `conduit` found on `PATH` -- it downloads a specific, pinned GoReleaser
release asset from GitHub Releases, verifies it against the release's
published `checksums.txt`, and caches the extracted binary under a per-version
directory in the user's cache dir. A second `local()` call with the same
version reuses the cached binary; no network access happens if it's already
present.

Artifact naming (confirmed against `.goreleaser.yml`, not guessed):
`conduit_<version>_<Os>_<Arch>.tar.gz` on macOS/Linux (`.zip` on Windows),
`Os` title-cased (`Darwin`/`Linux`/`Windows`), `Arch` as `x86_64` for amd64 or
`arm64` for arm64 -- e.g. `conduit_0.18.0_Darwin_arm64.tar.gz`.
"""

from __future__ import annotations

import hashlib
import io
import os
import platform
import stat
import tarfile
import tempfile
import urllib.request
import zipfile
from pathlib import Path
from typing import cast

import platformdirs

from conduit.errors import ConduitError

#: Default pinned version, used when `local()`/`ensure_binary()` are not told
#: otherwise. Bump deliberately on a release, never auto-resolved to
#: "latest" -- reproducibility over convenience (design doc, "Upgrade /
#: rollback": "each client library release documents its minimum supported
#: engine version").
DEFAULT_CONDUIT_VERSION = "0.18.0"

#: Overrides DEFAULT_CONDUIT_VERSION when set, without touching call sites.
_VERSION_ENV_VAR = "CONDUIT_CLIENT_ENGINE_VERSION"

_GITHUB_RELEASES = "https://github.com/ConduitIO/conduit/releases/download"


def resolve_version(version: str | None) -> str:
    """Resolve the effective `conduit` version, stripping a leading `v` if present.

    Precedence: explicit `version=` argument > `CONDUIT_CLIENT_ENGINE_VERSION`
    env var > :data:`DEFAULT_CONDUIT_VERSION`. Never resolves "latest" over
    the network -- see module docstring.
    """
    resolved = version or os.environ.get(_VERSION_ENV_VAR) or DEFAULT_CONDUIT_VERSION
    return resolved.removeprefix("v")


def _platform_triplet() -> tuple[str, str, str]:
    """Return (goreleaser_os, goreleaser_arch, archive_ext) for the current host.

    Raises:
        ConduitError: for a platform/arch this module doesn't know a
            published release asset name for, with code
            ``"client.unsupported_platform"`` -- fails fast and named, never
            a silent wrong download.
    """
    system = platform.system()
    machine = platform.machine().lower()

    os_map = {"Darwin": "Darwin", "Linux": "Linux", "Windows": "Windows"}
    arch_map = {"x86_64": "x86_64", "amd64": "x86_64", "arm64": "arm64", "aarch64": "arm64"}

    goreleaser_os = os_map.get(system)
    goreleaser_arch = arch_map.get(machine)
    if goreleaser_os is None or goreleaser_arch is None:
        raise ConduitError(
            f"no published conduit release asset known for platform "
            f"system={system!r} machine={machine!r}. Supported: "
            f"{sorted(os_map)} x {sorted(set(arch_map.values()))}.",
            code="client.unsupported_platform",
        )
    ext = "zip" if goreleaser_os == "Windows" else "tar.gz"
    return goreleaser_os, goreleaser_arch, ext


def _archive_name(version: str) -> str:
    """The exact GoReleaser archive filename for `version` on this platform."""
    goreleaser_os, goreleaser_arch, ext = _platform_triplet()
    return f"conduit_{version}_{goreleaser_os}_{goreleaser_arch}.{ext}"


def _binary_name() -> str:
    return "conduit.exe" if platform.system() == "Windows" else "conduit"


def cache_dir(version: str) -> Path:
    """The per-version cache directory the provisioned binary lives under."""
    return Path(platformdirs.user_cache_dir("conduit-client-python")) / "bin" / version


def cached_binary_path(version: str) -> Path:
    """Where the extracted binary would live for `version`, whether or not it exists yet."""
    return cache_dir(version) / _binary_name()


def _http_get(url: str) -> bytes:
    """Fetch `url`'s full response body. The sole network I/O in this module.

    Isolated into its own function so unit tests can monkeypatch
    ``conduit._provision._http_get`` and exercise the rest of
    :func:`ensure_binary` (version resolution, checksum verification,
    archive extraction, caching) without any real network access -- per the
    build task's gate: "provisioning version-resolution with a MOCKED
    download."
    """
    # Fixed https:// GitHub Releases URL built from this module's own constants,
    # never user input -- not a dynamic-URL SSRF surface.
    with urllib.request.urlopen(url, timeout=30) as resp:
        return cast(bytes, resp.read())


def _verify_checksum(archive_bytes: bytes, checksums_txt: bytes, archive_name: str) -> None:
    """Verify `archive_bytes` against the `<sha256>  <filename>` line for `archive_name`.

    Raises:
        ConduitError: (code ``"client.checksum_mismatch"``) if the archive's
            filename isn't listed, or its digest doesn't match -- never
            silently accepted.
    """
    expected: str | None = None
    for line in checksums_txt.decode("utf-8").splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[1] == archive_name:
            expected = parts[0]
            break
    if expected is None:
        raise ConduitError(
            f"{archive_name!r} not listed in the release's checksums.txt -- "
            "refusing to trust an unverifiable download.",
            code="client.checksum_mismatch",
        )
    actual = hashlib.sha256(archive_bytes).hexdigest()
    if actual != expected:
        raise ConduitError(
            f"checksum mismatch for {archive_name!r}: expected {expected}, got {actual}. "
            "Refusing to use a download that doesn't match the published release checksum.",
            code="client.checksum_mismatch",
        )


def _extract_binary(archive_bytes: bytes, archive_ext: str, binary_name: str) -> bytes:
    """Extract just `binary_name` from the archive, returning its raw bytes."""
    if archive_ext == "zip":
        with zipfile.ZipFile(io.BytesIO(archive_bytes)) as zf:
            for name in zf.namelist():
                if Path(name).name == binary_name:
                    return zf.read(name)
    else:
        with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:gz") as tf:
            for member in tf.getmembers():
                if Path(member.name).name == binary_name and member.isfile():
                    extracted = tf.extractfile(member)
                    if extracted is not None:
                        return extracted.read()
    raise ConduitError(
        f"archive did not contain a {binary_name!r} entry -- release asset layout may "
        "have changed; this client's extraction logic needs updating.",
        code="client.malformed_release_asset",
    )


def ensure_binary(version: str | None = None) -> Path:
    """Ensure a verified `conduit` binary for `version` is cached locally, and return its path.

    Downloads nothing if already cached. Otherwise: downloads the platform's
    release archive and `checksums.txt`, verifies the archive's SHA-256
    against the published checksum, extracts the single `conduit`
    (`conduit.exe` on Windows) binary, marks it executable, and atomically
    installs it into the per-version cache directory (download-then-rename,
    never a partially-written file left at the final path).

    Args:
        version: exact version to provision (a leading `v` is stripped if
            present); resolved via :func:`resolve_version` if `None`.

    Returns:
        Absolute path to the cached, executable `conduit` binary.

    Raises:
        ConduitError: on an unsupported platform, a download failure, a
            checksum mismatch, or a malformed archive -- never a silent
            fallback to some other binary.
    """
    resolved_version = resolve_version(version)
    target = cached_binary_path(resolved_version)
    if target.exists():
        return target

    archive_name = _archive_name(resolved_version)
    base_url = f"{_GITHUB_RELEASES}/v{resolved_version}"

    try:
        archive_bytes = _http_get(f"{base_url}/{archive_name}")
        checksums_bytes = _http_get(f"{base_url}/checksums.txt")
    except OSError as e:
        raise ConduitError(
            f"failed to download conduit v{resolved_version} release asset "
            f"({archive_name}) from GitHub Releases: {e}",
            code="client.download_failed",
        ) from e

    _verify_checksum(archive_bytes, checksums_bytes, archive_name)

    _, _, archive_ext = _platform_triplet()
    binary_bytes = _extract_binary(archive_bytes, archive_ext, _binary_name())

    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path_str = tempfile.mkstemp(dir=target.parent, prefix=".conduit-download-")
    tmp_path = Path(tmp_path_str)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(binary_bytes)
        tmp_path.chmod(tmp_path.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
        tmp_path.replace(target)  # atomic on the same filesystem
    finally:
        tmp_path.unlink(missing_ok=True)

    return target
