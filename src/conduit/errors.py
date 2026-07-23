"""Connector-author-facing exceptions.

Python favors exceptions over Go's ``(n, err)`` return convention for error
propagation. See ``docs/design/20260707-python-connector-sdk.md`` §2.5 for the
full rationale, including why this is a genuine simplification (not just a
stylistic swap) over Go's ``Destination.Write(ctx, batch) (n int, err error)``
contract, which requires the Go SDK to defend an invariant at runtime
(``destination.go:345-350``, re-verified ▶ MUST-FIX 1) that this module makes
structurally unrepresentable instead.
"""

from __future__ import annotations

from collections.abc import Mapping, Set


class ConnectorError(Exception):
    """Base exception for connector-raised errors surfaced over the wire.

    Any exception raised from author code that is not :class:`BackoffRetry`
    (source) or :class:`BatchWriteError` (destination) propagates to Conduit
    as a gRPC ``INTERNAL`` status with the exception's string as detail --
    ``ConnectorError`` is not required for that, any exception works. This
    class exists for authors who want to attach a stable error ``code``.

    Per §2.5: the connector protocol has no stable plugin-originated error
    code scheme today (one is flagged as landing around v0.16 of Conduit
    itself). ``code`` is reserved for that; until the protocol adds a wire
    slot for it, it is always ``None`` from this SDK's perspective and is
    **not** transmitted -- only ``str(exception)`` crosses the wire as the
    gRPC status detail.
    """

    def __init__(self, message: str, *, code: str | None = None) -> None:
        """Initialize with a human-readable message and optional error code.

        Args:
            message: human-readable description, becomes ``str(self)``.
            code: reserved for a future stable error-code scheme; unused on
                the wire today (see class docstring).
        """
        super().__init__(message)
        self.code = code


class BackoffRetry(ConnectorError):
    """Raised by ``Source.read()`` to mean "no record right now, retry".

    Direct analog of the Go SDK's ``ErrBackoffRetry``, consumed by the read
    loop with a ``Factor=2, Min=100ms, Max=5s`` backoff
    (``source.go:270-295``, re-verified ▶ MUST-FIX 1 in the design doc -- a
    genuinely serial loop with no concurrent read invocation, so the Python
    SDK reusing the identical constants is real behavioral parity). The
    SDK's own read loop sleeps between retries -- authors must not also
    ``asyncio.sleep()`` (or block) before raising this, or they double the
    intended backoff (see the design doc §2.7 worked example's note on this
    exact mistake).
    """

    def __init__(self, message: str = "no record available, retry with backoff") -> None:
        """Initialize with an optional diagnostic message.

        Args:
            message: human-readable reason there's nothing to read right
                now; purely diagnostic, never required by callers.
        """
        super().__init__(message)


class BatchWriteError(ConnectorError):
    """Raised by ``Destination.write()`` to report a partial-batch failure.

    This is the SDK's fix for the B1 data-loss blocker identified in the
    2026-07-07 design review (see
    ``docs/design/20260707-python-connector-sdk.md`` §2.5 for the full
    statement). Go's ``(n, err)`` write contract makes "everything not
    explicitly marked as failed was successfully, durably written" an easy
    mistake to fall into -- easy enough that the Go SDK itself runs a
    defensive runtime guard against it on every batch write
    (``destination.go:345-350``, re-verified ▶ MUST-FIX 1, pasted verbatim
    in the design doc). This class makes the equivalent mistake **impossible
    to construct** in the first place, not merely detected after the fact:
    the accounting of which indices succeeded and which failed must be
    supplied, in full, at construction time, or ``__init__`` raises
    ``ValueError``.

    Two ways to construct it:

    1. ``BatchWriteError(batch_size, written=N)`` -- the common case, a
       contiguous success prefix (Go's ``n``): "everything up to index
       ``N - 1`` succeeded, everything from ``N`` on failed." Every index
       ``>= N`` is recorded as a generic failure.
    2. ``BatchWriteError(batch_size, success={...}, failures={...})`` -- an
       explicit, non-contiguous accounting. ``success`` and ``failures``
       together must cover every index in ``range(batch_size)`` exactly
       once (no gaps, no overlaps).

    **An index present in neither accounting is never assumed successful.**
    If ``success``/``failures`` don't exhaustively and disjointly cover
    every index, construction fails closed with ``ValueError`` -- this is
    the concrete mechanism behind the design doc's "banned by construction,
    not merely documented" claim. The SDK's destination adapter
    (:mod:`conduit.destination`) has no code path that computes "ack
    everything not explicitly marked as failed"; it only ever acks indices
    present in ``self.success``.
    """

    def __init__(
        self,
        batch_size: int,
        *,
        written: int | None = None,
        success: Set[int] | None = None,
        failures: Mapping[int, BaseException] | None = None,
    ) -> None:
        """Validate and record an exhaustive per-index write outcome.

        Args:
            batch_size: number of records in the batch this error concerns.
                Every index accounting is checked against
                ``range(batch_size)``.
            written: contiguous success-prefix count (Go's ``n``). Mutually
                exclusive with ``success``/``failures``.
            success: explicit set of successfully, durably written indices.
                Must be paired with ``failures``.
            failures: explicit mapping of failed index -> the exception that
                caused that index's failure. Must be paired with ``success``.

        Raises:
            ValueError: if the accounting is missing, incomplete, overlaps
                between ``success``/``failures``, or references indices
                outside ``range(batch_size)``. This is the fail-closed rule
                from §2.5: incompleteness is itself a construction-time
                error, never silently resolved by assuming missing indices
                succeeded.
        """
        if written is not None and (success is not None or failures is not None):
            raise ValueError(
                "BatchWriteError: pass either `written=` or `success=`/`failures=`, "
                "not both -- the two forms are mutually exclusive ways to supply "
                "the same exhaustive per-index accounting"
            )

        resolved_success: set[int]
        resolved_failures: dict[int, BaseException]

        if written is not None:
            if not 0 <= written <= batch_size:
                raise ValueError(
                    f"BatchWriteError: written={written} is out of range for "
                    f"batch_size={batch_size} (must satisfy 0 <= written <= batch_size)"
                )
            resolved_success = set(range(written))
            resolved_failures = {
                i: RuntimeError(
                    "batch write reported only a partial success prefix "
                    f"(written={written}); index {i} was not reached"
                )
                for i in range(written, batch_size)
            }
        else:
            if success is None or failures is None:
                raise ValueError(
                    "BatchWriteError: must supply either `written=`, or both "
                    "`success=` and `failures=` explicitly. An omitted accounting "
                    "is never treated as an implicit 'everything else succeeded' -- "
                    "that is exactly the B1 data-loss bug this exception exists to "
                    "make unrepresentable (docs/design/20260707-python-connector-sdk.md §2.5)"
                )
            resolved_success = set(success)
            resolved_failures = dict(failures)

            overlap = resolved_success & resolved_failures.keys()
            if overlap:
                raise ValueError(
                    f"BatchWriteError: indices {sorted(overlap)} appear in both "
                    "`success` and `failures` -- each index must be accounted "
                    "exactly once"
                )

            all_indices = set(range(batch_size))
            covered = resolved_success | resolved_failures.keys()
            missing = all_indices - covered
            if missing:
                raise ValueError(
                    f"BatchWriteError: indices {sorted(missing)} of batch_size="
                    f"{batch_size} are unaccounted for in either `success` or "
                    "`failures`. The accounting must be exhaustive: an index "
                    "present in neither set is a bug in the connector's write(), "
                    "and must never be assumed to have succeeded (fail-closed, "
                    "§2.5 B1 fix)"
                )
            out_of_range = covered - all_indices
            if out_of_range:
                raise ValueError(
                    f"BatchWriteError: indices {sorted(out_of_range)} are outside "
                    f"range(batch_size={batch_size})"
                )

        self.batch_size = batch_size
        self.success: frozenset[int] = frozenset(resolved_success)
        self.failures: dict[int, BaseException] = resolved_failures

        summary = "; ".join(f"index {i}: {e}" for i, e in sorted(self.failures.items()))
        super().__init__(
            f"partial batch write failure: {len(self.failures)}/{batch_size} "
            f"record(s) failed ({summary})"
            if summary
            else "partial batch write failure"
        )
