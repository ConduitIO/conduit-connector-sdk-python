"""Tests for :mod:`conduit.errors` -- primarily ``BatchWriteError``'s B1 fix.

The construction-time validation here is the concrete, automated form of
the design doc's B1 fix (§2.5): "an index present in neither the success
accounting nor the failure accounting is treated as failed, never as
successful" is enforced by making the incomplete/inconsistent construction
itself impossible (``ValueError``), not merely documented.
"""

from __future__ import annotations

import pytest

from conduit.errors import BackoffRetry, BatchWriteError, ConnectorError


class TestBatchWriteErrorWrittenPrefix:
    def test_written_prefix_marks_exactly_that_range_successful(self) -> None:
        err = BatchWriteError(5, written=3)
        assert err.success == {0, 1, 2}
        assert set(err.failures) == {3, 4}

    def test_written_zero_means_nothing_succeeded(self) -> None:
        err = BatchWriteError(3, written=0)
        assert err.success == set()
        assert set(err.failures) == {0, 1, 2}

    def test_written_equal_to_batch_size_means_everything_succeeded(self) -> None:
        err = BatchWriteError(3, written=3)
        assert err.success == {0, 1, 2}
        assert err.failures == {}

    def test_written_negative_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="out of range"):
            BatchWriteError(3, written=-1)

    def test_written_greater_than_batch_size_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="out of range"):
            BatchWriteError(3, written=4)


class TestBatchWriteErrorExplicitAccounting:
    def test_exhaustive_disjoint_accounting_is_accepted(self) -> None:
        err = BatchWriteError(4, success={0, 2}, failures={1: ValueError("x"), 3: ValueError("y")})
        assert err.success == {0, 2}
        assert set(err.failures) == {1, 3}

    def test_missing_index_raises_value_error_fail_closed(self) -> None:
        """The core B1 assertion: an unaccounted index must raise, never silently succeed."""
        with pytest.raises(ValueError, match="unaccounted"):
            BatchWriteError(4, success={0, 2}, failures={1: ValueError("x")})  # index 3 missing

    def test_overlapping_index_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="both"):
            BatchWriteError(2, success={0, 1}, failures={1: ValueError("x")})

    def test_out_of_range_index_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match=r"outside range|unaccounted"):
            BatchWriteError(2, success={0, 1, 5}, failures={})

    def test_success_without_failures_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="must supply"):
            BatchWriteError(2, success={0, 1})  # type: ignore[call-overload]

    def test_failures_without_success_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="must supply"):
            BatchWriteError(2, failures={0: ValueError("x")})  # type: ignore[call-overload]

    def test_neither_written_nor_accounting_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="must supply"):
            BatchWriteError(2)

    def test_both_written_and_accounting_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="not both"):
            BatchWriteError(2, written=1, success={1}, failures={0: ValueError("x")})


class TestBatchWriteErrorPartial:
    """``BatchWriteError.partial()`` -- the recommended way to raise a partial-batch failure."""

    def test_written_prefix_is_acked_the_rest_gets_the_real_cause(self) -> None:
        cause = ConnectionError("destination went away mid-batch")
        err = BatchWriteError.partial(5, written=2, cause=cause)
        assert err.success == {0, 1}
        assert set(err.failures) == {2, 3, 4}
        # Every failed index carries the REAL cause, not a generic
        # placeholder -- the whole point of `.partial()` over the plain
        # `written=` constructor.
        assert all(err.failures[i] is cause for i in (2, 3, 4))

    def test_cause_message_reaches_str_of_the_error(self) -> None:
        cause = TimeoutError("upstream timed out")
        err = BatchWriteError.partial(3, written=1, cause=cause)
        assert "upstream timed out" in str(err)

    def test_written_zero_means_nothing_succeeded(self) -> None:
        cause = RuntimeError("boom")
        err = BatchWriteError.partial(3, written=0, cause=cause)
        assert err.success == set()
        assert set(err.failures) == {0, 1, 2}

    def test_written_equal_to_batch_size_means_everything_succeeded(self) -> None:
        cause = RuntimeError("unreachable in practice, but must not crash")
        err = BatchWriteError.partial(3, written=3, cause=cause)
        assert err.success == {0, 1, 2}
        assert err.failures == {}

    def test_written_out_of_range_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="out of range"):
            BatchWriteError.partial(3, written=4, cause=RuntimeError("x"))

    def test_result_is_a_real_exhaustively_validated_batch_write_error(self) -> None:
        """``.partial()`` reuses the same validated constructor path, not a shortcut around it."""
        err = BatchWriteError.partial(4, written=2, cause=RuntimeError("x"))
        assert isinstance(err, BatchWriteError)
        assert err.batch_size == 4
        # Exhaustiveness already proven by BatchWriteError.__init__ itself
        # (see TestBatchWriteErrorExplicitAccounting) -- this just checks
        # .partial() actually goes through that path rather than bypassing it.
        assert err.success | set(err.failures) == {0, 1, 2, 3}


class TestBackoffRetry:
    def test_is_a_connector_error(self) -> None:
        assert isinstance(BackoffRetry(), ConnectorError)

    def test_default_message_is_diagnostic_only(self) -> None:
        assert "retry" in str(BackoffRetry())


class TestConnectorError:
    def test_code_defaults_to_none(self) -> None:
        assert ConnectorError("boom").code is None

    def test_code_is_stored_when_given(self) -> None:
        assert ConnectorError("boom", code="E001").code == "E001"
