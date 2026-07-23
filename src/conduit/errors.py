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

import pydantic


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


def format_validation_error(exc: pydantic.ValidationError) -> str:
    """Format a pydantic ``ValidationError`` as a concise, per-field detail string.

    Used by the ``Configure`` RPC handlers (:mod:`conduit.source`/
    :mod:`conduit.destination`) to build the gRPC ``INVALID_ARGUMENT``
    status detail explicitly, rather than relying on ``grpc.aio``'s own
    generic "Unexpected <exception class>: ..." wrapping of an uncaught
    exception (``StatusCode.UNKNOWN``) -- per ``CLAUDE.md``'s "errors are
    API, actionable" standard, an author (or Conduit's own error surface)
    should be able to see exactly which field failed and why, not an
    opaque blob, and the SDK should own that contract explicitly rather
    than depend on incidental library formatting.

    Args:
        exc: the validation error to format.

    Returns:
        A multi-line string: one summary line, then one ``<field path>:
        <message>`` line per error. Omits pydantic's "For further
        information visit ..." doc-link lines (``include_url=False``) --
        noise in a gRPC status detail, not useful to an operator reading a
        pipeline's error log.
    """
    lines = [f"invalid config ({exc.error_count()} error(s)):"]
    for error in exc.errors(include_url=False):
        loc = ".".join(str(part) for part in error["loc"]) or "<root>"
        lines.append(f"  {loc}: {error['msg']}")
    return "\n".join(lines)


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

    Three ways to construct it, from most to least recommended:

    1. **``BatchWriteError.partial(batch_size, written=N, cause=exc)``** --
       the recommended way to raise the common case (a contiguous success
       prefix, Go's ``n``): "everything up to index ``N - 1`` succeeded,
       everything from ``N`` on failed because of ``exc``." Every index
       ``>= N`` is recorded as failed with your real ``cause`` exception,
       not a generic placeholder -- see :meth:`partial`.
    2. ``BatchWriteError(batch_size, written=N)`` -- same contiguous-prefix
       shape without a specific cause; failed indices get a generic
       internal message. Use :meth:`partial` instead when you have the
       real exception that stopped the write.
    3. ``BatchWriteError(batch_size, success={...}, failures={...})`` -- an
       explicit, non-contiguous accounting, for the less common case where
       failures aren't a simple prefix. ``success`` and ``failures``
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

    @classmethod
    def partial(cls, batch_size: int, *, written: int, cause: BaseException) -> BatchWriteError:
        """Construct the common contiguous-prefix case, with a real cause.

        This is the **recommended** way to raise a partial-batch failure:
        ``raise BatchWriteError.partial(len(records), written=3, cause=exc)``.
        Equivalent to ``BatchWriteError(batch_size, written=written)``,
        except every index past the written prefix is recorded as having
        failed with ``cause`` itself -- the real exception your ``write()``
        caught -- rather than a generic internal "not reached" placeholder.
        This means the ack's error detail that eventually reaches Conduit
        (and whoever's reading the pipeline's error log) reflects what
        actually went wrong, not just that something did.

        Authors never hand-build the ``success``/``failures``
        set/mapping for this common case -- this classmethod does it,
        reusing the exhaustive-accounting constructor path (already
        validated) under the hood.

        Args:
            batch_size: number of records in the batch -- typically
                ``len(records)`` from your ``write(self, records)``.
            written: contiguous success-prefix count (Go's ``n``):
                indices ``[0, written)`` succeeded.
            cause: the exception that caused the write to stop after
                ``written`` records. Recorded as every index ``>= written``'s
                failure reason.

        Returns:
            A fully validated, ready-to-raise ``BatchWriteError``.

        Raises:
            ValueError: if ``written`` is out of range for ``batch_size``
                (see :meth:`__init__`).
        """
        if not 0 <= written <= batch_size:
            raise ValueError(
                f"BatchWriteError.partial: written={written} is out of range for "
                f"batch_size={batch_size} (must satisfy 0 <= written <= batch_size)"
            )
        success = set(range(written))
        failures: dict[int, BaseException] = dict.fromkeys(range(written, batch_size), cause)
        return cls(batch_size, success=success, failures=failures)
