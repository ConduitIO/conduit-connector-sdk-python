"""Binary provisioning: version resolution, checksum verification, extraction.

All network I/O is mocked (``conduit._provision._http_get`` monkeypatched) --
per the build task's gate: "provisioning version-resolution with a MOCKED
download." :mod:`tests.integration.test_local_generator_log` covers the real,
unmocked download-and-launch path and is honestly skipped where there's no
network.
"""

from __future__ import annotations

import hashlib
import io
import os
import platform
import tarfile
from pathlib import Path

import pytest

from conduit import _provision
from conduit.errors import ConduitError


def test_resolve_version_strips_leading_v() -> None:
    assert _provision.resolve_version("v0.19.0") == "0.19.0"


def test_resolve_version_explicit_wins_over_default() -> None:
    assert _provision.resolve_version("0.20.0") == "0.20.0"


def test_resolve_version_falls_back_to_default() -> None:
    assert _provision.resolve_version(None) == _provision.DEFAULT_CONDUIT_VERSION


def test_resolve_version_env_var_overrides_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CONDUIT_CLIENT_ENGINE_VERSION", "v0.21.0")
    assert _provision.resolve_version(None) == "0.21.0"


def test_resolve_version_explicit_arg_beats_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CONDUIT_CLIENT_ENGINE_VERSION", "0.21.0")
    assert _provision.resolve_version("0.22.0") == "0.22.0"


def test_platform_triplet_known_platform(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(platform, "system", lambda: "Darwin")
    monkeypatch.setattr(platform, "machine", lambda: "arm64")
    assert _provision._platform_triplet() == ("Darwin", "arm64", "tar.gz")


def test_platform_triplet_linux_amd64(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(platform, "system", lambda: "Linux")
    monkeypatch.setattr(platform, "machine", lambda: "x86_64")
    assert _provision._platform_triplet() == ("Linux", "x86_64", "tar.gz")


def test_platform_triplet_windows_uses_zip(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(platform, "system", lambda: "Windows")
    monkeypatch.setattr(platform, "machine", lambda: "amd64")
    assert _provision._platform_triplet() == ("Windows", "x86_64", "zip")


def test_platform_triplet_unsupported_raises_conduit_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(platform, "system", lambda: "Plan9")
    monkeypatch.setattr(platform, "machine", lambda: "risc-v")
    with pytest.raises(ConduitError) as exc_info:
        _provision._platform_triplet()
    assert exc_info.value.code == "client.unsupported_platform"


def _make_tar_gz(binary_name: str, binary_content: bytes) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        info = tarfile.TarInfo(name=binary_name)
        info.size = len(binary_content)
        tf.addfile(info, io.BytesIO(binary_content))
    return buf.getvalue()


def test_ensure_binary_downloads_verifies_and_caches(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(platform, "system", lambda: "Darwin")
    monkeypatch.setattr(platform, "machine", lambda: "arm64")
    monkeypatch.setattr(_provision.platformdirs, "user_cache_dir", lambda _name: str(tmp_path))

    binary_bytes = b"#!/bin/sh\necho fake-conduit\n"
    archive_bytes = _make_tar_gz("conduit", binary_bytes)
    archive_name = "conduit_1.2.3_Darwin_arm64.tar.gz"
    checksum = hashlib.sha256(archive_bytes).hexdigest()
    checksums_txt = f"{checksum}  {archive_name}\ndeadbeef  some_other_file.tar.gz\n".encode()

    requested_urls: list[str] = []

    def fake_http_get(url: str) -> bytes:
        requested_urls.append(url)
        if url.endswith("checksums.txt"):
            return checksums_txt
        assert url.endswith(archive_name)
        return archive_bytes

    monkeypatch.setattr(_provision, "_http_get", fake_http_get)

    path = _provision.ensure_binary("1.2.3")

    assert path == _provision.cached_binary_path("1.2.3")
    assert path.exists()
    assert path.read_bytes() == binary_bytes
    assert os.access(path, os.X_OK)
    assert any(archive_name in u for u in requested_urls)
    assert any(u.endswith("checksums.txt") for u in requested_urls)

    # Second call: cached, no network access at all.
    monkeypatch.setattr(
        _provision,
        "_http_get",
        lambda _url: (_ for _ in ()).throw(AssertionError("should not re-download")),
    )
    cached_path = _provision.ensure_binary("1.2.3")
    assert cached_path == path


def test_ensure_binary_rejects_checksum_mismatch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(platform, "system", lambda: "Linux")
    monkeypatch.setattr(platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(_provision.platformdirs, "user_cache_dir", lambda _name: str(tmp_path))

    archive_bytes = _make_tar_gz("conduit", b"binary-content")
    archive_name = "conduit_9.9.9_Linux_x86_64.tar.gz"
    wrong_digest = "0" * 64
    checksums_txt = f"{wrong_digest}  {archive_name}\n".encode()

    def fake_http_get(url: str) -> bytes:
        return checksums_txt if url.endswith("checksums.txt") else archive_bytes

    monkeypatch.setattr(_provision, "_http_get", fake_http_get)

    with pytest.raises(ConduitError) as exc_info:
        _provision.ensure_binary("9.9.9")
    assert exc_info.value.code == "client.checksum_mismatch"
    assert not _provision.cached_binary_path("9.9.9").exists()


def test_ensure_binary_rejects_unlisted_archive(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(platform, "system", lambda: "Linux")
    monkeypatch.setattr(platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(_provision.platformdirs, "user_cache_dir", lambda _name: str(tmp_path))

    archive_bytes = _make_tar_gz("conduit", b"binary-content")
    checksums_txt = b"deadbeef  some_totally_different_file.tar.gz\n"

    monkeypatch.setattr(
        _provision,
        "_http_get",
        lambda url: checksums_txt if url.endswith("checksums.txt") else archive_bytes,
    )

    with pytest.raises(ConduitError) as exc_info:
        _provision.ensure_binary("5.5.5")
    assert exc_info.value.code == "client.checksum_mismatch"


def test_ensure_binary_download_failure_wrapped_as_conduit_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(platform, "system", lambda: "Linux")
    monkeypatch.setattr(platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(_provision.platformdirs, "user_cache_dir", lambda _name: str(tmp_path))

    def fake_http_get(_url: str) -> bytes:
        raise OSError("network unreachable (simulated)")

    monkeypatch.setattr(_provision, "_http_get", fake_http_get)

    with pytest.raises(ConduitError) as exc_info:
        _provision.ensure_binary("1.0.0")
    assert exc_info.value.code == "client.download_failed"


def test_ensure_binary_rejects_malformed_archive(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(platform, "system", lambda: "Linux")
    monkeypatch.setattr(platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(_provision.platformdirs, "user_cache_dir", lambda _name: str(tmp_path))

    # A tar.gz with no `conduit` entry at all.
    archive_bytes = _make_tar_gz("some-other-file", b"not the binary")
    archive_name = "conduit_2.0.0_Linux_x86_64.tar.gz"
    checksum = hashlib.sha256(archive_bytes).hexdigest()
    checksums_txt = f"{checksum}  {archive_name}\n".encode()

    monkeypatch.setattr(
        _provision,
        "_http_get",
        lambda url: checksums_txt if url.endswith("checksums.txt") else archive_bytes,
    )

    with pytest.raises(ConduitError) as exc_info:
        _provision.ensure_binary("2.0.0")
    assert exc_info.value.code == "client.malformed_release_asset"
