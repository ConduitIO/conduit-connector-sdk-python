"""Tests for :mod:`conduit._batch` -- ``sdk.batch.size``/``sdk.batch.delay``.

Covers the three independently interesting pieces: :class:`BatchConfig`'s
exact Go-matching activation threshold, :func:`extract_batch_config`'s
stable-coded validation (the config-validation-error-codes requirement),
and :func:`collect_batches`'s size/delay/flush-on-end accumulation logic
(the actual batching semantics -- this is the SDK-adapter-level guarantee
test, exercised directly against the shared primitive rather than only
indirectly through ``_SourceServicer``/``_DestinationServicer``, mirroring
how ``tests/test_errors.py`` tests ``BatchWriteError`` directly).
"""

from __future__ import annotations

import asyncio
import datetime

import pytest

from conduit._batch import (
    BATCH_DELAY_KEY,
    BATCH_SIZE_KEY,
    BatchConfig,
    collect_batches,
    extract_batch_config,
    sdk_batch_parameters,
)
from conduit.errors import (
    INVALID_BATCH_DELAY_CODE,
    INVALID_BATCH_SIZE_CODE,
    ConfigValidationError,
)
from config.v1 import parameter_pb2


class TestBatchConfigEnabled:
    """The exact Go activation threshold: `size > 1 or delay > 0`."""

    def test_default_is_disabled(self) -> None:
        assert BatchConfig().enabled is False

    def test_size_zero_is_disabled(self) -> None:
        assert BatchConfig(size=0).enabled is False

    def test_size_one_is_disabled(self) -> None:
        """A "batch" of one record is passthrough, exactly like Go (`destination.go:182`)."""
        assert BatchConfig(size=1).enabled is False

    def test_size_two_is_enabled(self) -> None:
        assert BatchConfig(size=2).enabled is True

    def test_delay_zero_is_disabled(self) -> None:
        assert BatchConfig(delay=datetime.timedelta(0)).enabled is False

    def test_any_positive_delay_is_enabled(self) -> None:
        assert BatchConfig(delay=datetime.timedelta(milliseconds=1)).enabled is True

    def test_size_and_delay_together_is_enabled(self) -> None:
        assert BatchConfig(size=10, delay=datetime.timedelta(seconds=1)).enabled is True


class TestSdkBatchParameters:
    def test_exposes_exactly_the_two_keys(self) -> None:
        params = sdk_batch_parameters()
        assert set(params) == {BATCH_SIZE_KEY, BATCH_DELAY_KEY}

    def test_size_param_is_int_with_default_zero_and_nonnegative_validation(self) -> None:
        param = sdk_batch_parameters()[BATCH_SIZE_KEY]
        assert param.type == parameter_pb2.Parameter.TYPE_INT
        assert param.default == "0"
        assert any(
            v.type == parameter_pb2.Validation.TYPE_GREATER_THAN and v.value == "-1"
            for v in param.validations
        )

    def test_delay_param_is_duration_with_default_zero(self) -> None:
        param = sdk_batch_parameters()[BATCH_DELAY_KEY]
        assert param.type == parameter_pb2.Parameter.TYPE_DURATION
        assert param.default == "0s"


class TestExtractBatchConfig:
    def test_absent_keys_default_to_disabled_config(self) -> None:
        config, remaining = extract_batch_config({"url": "https://example.com"})
        assert config == BatchConfig()
        assert remaining == {"url": "https://example.com"}

    def test_valid_size_and_delay_are_parsed_and_stripped(self) -> None:
        config, remaining = extract_batch_config(
            {"url": "x", BATCH_SIZE_KEY: "50", BATCH_DELAY_KEY: "2s"}
        )
        assert config == BatchConfig(size=50, delay=datetime.timedelta(seconds=2))
        assert remaining == {"url": "x"}
        assert BATCH_SIZE_KEY not in remaining
        assert BATCH_DELAY_KEY not in remaining

    def test_original_mapping_is_not_mutated(self) -> None:
        original = {BATCH_SIZE_KEY: "10"}
        extract_batch_config(original)
        assert original == {BATCH_SIZE_KEY: "10"}, "extract_batch_config must not mutate its input"

    def test_invalid_size_raises_config_validation_error_with_stable_code(self) -> None:
        with pytest.raises(ConfigValidationError) as exc_info:
            extract_batch_config({BATCH_SIZE_KEY: "not-an-int"})
        errors = exc_info.value.errors
        assert len(errors) == 1
        assert errors[0].field == BATCH_SIZE_KEY
        assert errors[0].code == INVALID_BATCH_SIZE_CODE

    def test_negative_size_raises_config_validation_error_with_stable_code(self) -> None:
        with pytest.raises(ConfigValidationError) as exc_info:
            extract_batch_config({BATCH_SIZE_KEY: "-1"})
        assert exc_info.value.errors[0].code == INVALID_BATCH_SIZE_CODE

    def test_invalid_delay_raises_config_validation_error_with_stable_code(self) -> None:
        with pytest.raises(ConfigValidationError) as exc_info:
            extract_batch_config({BATCH_DELAY_KEY: "not-a-duration"})
        errors = exc_info.value.errors
        assert len(errors) == 1
        assert errors[0].field == BATCH_DELAY_KEY
        assert errors[0].code == INVALID_BATCH_DELAY_CODE

    def test_negative_delay_raises_config_validation_error_with_stable_code(self) -> None:
        with pytest.raises(ConfigValidationError) as exc_info:
            extract_batch_config({BATCH_DELAY_KEY: "-5s"})
        assert exc_info.value.errors[0].code == INVALID_BATCH_DELAY_CODE

    def test_both_invalid_reports_both_errors_exhaustively(self) -> None:
        """Not fail-fast on the first bad key -- both are reported.

        Mirrors ``BatchWriteError``'s exhaustive accounting elsewhere.
        """
        with pytest.raises(ConfigValidationError) as exc_info:
            extract_batch_config({BATCH_SIZE_KEY: "nope", BATCH_DELAY_KEY: "also-nope"})
        codes = {e.code for e in exc_info.value.errors}
        assert codes == {INVALID_BATCH_SIZE_CODE, INVALID_BATCH_DELAY_CODE}

    def test_assert_on_code_not_message_text(self) -> None:
        """The exact property WS2 asks for: codes are the stable contract, not message wording."""
        try:
            extract_batch_config({BATCH_SIZE_KEY: "bogus"})
        except ConfigValidationError as exc:
            assert exc.errors[0].code == INVALID_BATCH_SIZE_CODE
        else:
            pytest.fail("expected ConfigValidationError")


async def _alist(aiter: object) -> list[list[int]]:
    out: list[list[int]] = []
    async for batch in aiter:  # type: ignore[attr-defined]
        out.append(batch)
    return out


class TestCollectBatchesSizeTriggered:
    async def test_flushes_exactly_at_size_threshold(self) -> None:
        async def items() -> object:
            for i in range(6):
                yield i

        batches = await _alist(collect_batches(items(), config=BatchConfig(size=2)))
        assert batches == [[0, 1], [2, 3], [4, 5]]

    async def test_final_partial_batch_is_flushed_on_stream_end(self) -> None:
        async def items() -> object:
            for i in range(5):
                yield i

        batches = await _alist(collect_batches(items(), config=BatchConfig(size=2)))
        assert batches == [[0, 1], [2, 3], [4]]

    async def test_empty_stream_yields_no_batches(self) -> None:
        async def items() -> object:
            return
            yield  # pragma: no cover

        batches = await _alist(collect_batches(items(), config=BatchConfig(size=2)))
        assert batches == []

    async def test_size_zero_never_flushes_on_size_only_on_stream_end(self) -> None:
        """`size=0` means "no size limit" -- everything accumulates into one final batch."""

        async def items() -> object:
            for i in range(4):
                yield i

        batches = await _alist(
            collect_batches(items(), config=BatchConfig(delay=datetime.timedelta(seconds=10)))
        )
        assert batches == [[0, 1, 2, 3]]


class TestCollectBatchesDelayTriggered:
    async def test_flushes_on_delay_even_below_size_threshold(self) -> None:
        released = asyncio.Event()

        async def items() -> object:
            yield 1
            yield 2
            await released.wait()  # block well past the delay, so it fires first
            yield 3

        config = BatchConfig(size=100, delay=datetime.timedelta(milliseconds=20))
        batches: list[list[int]] = []
        agen = collect_batches(items(), config=config)
        async for batch in agen:
            batches.append(batch)
            if len(batches) == 1:
                released.set()
        assert batches[0] == [1, 2]
        assert batches[-1] == [3]

    async def test_delay_timer_only_starts_after_first_item(self) -> None:
        """No premature flush of an empty buffer while waiting for the first item."""

        async def items() -> object:
            await asyncio.sleep(0.05)  # longer than the delay below
            yield 1

        config = BatchConfig(delay=datetime.timedelta(milliseconds=10))
        batches = await _alist(collect_batches(items(), config=config))
        # Exactly one batch (the single item flushed on stream end), not an
        # earlier empty batch from the delay timer firing before any item
        # arrived.
        assert batches == [[1]]


class TestCollectBatchesCleanup:
    async def test_cancelling_mid_wait_does_not_raise(self) -> None:
        """Closing the generator early (simulating an abandoned consumer) cleans up tasks.

        The first batch (``[1]``) flushes quickly via the short delay timer.
        Awaiting the *second* batch then blocks inside ``collect_batches``'s
        ``asyncio.wait`` on the still-pending "next item" task (``items()``
        is stuck in ``asyncio.sleep(10)``) -- closing the generator from
        there must cancel that pending task and return without raising or
        hanging, per the module's documented "cleanup only, no yield in
        finally" contract.
        """

        async def items() -> object:
            yield 1
            await asyncio.sleep(10)  # still pending when aclose() is called
            yield 2  # pragma: no cover

        agen = collect_batches(
            items(), config=BatchConfig(size=100, delay=datetime.timedelta(milliseconds=10))
        )
        first = await agen.__anext__()
        assert first == [1]

        # A pending next-item task now exists (blocked in items()'s sleep);
        # give the event loop one more tick so collect_batches has actually
        # re-entered its `await asyncio.wait(...)` for item 2 before closing.
        await asyncio.sleep(0)
        await asyncio.wait_for(agen.aclose(), timeout=1.0)  # must not raise, must not hang
