"""Dual sync/async method dispatch shared by ``Source`` and ``Destination``.

Per ``docs/design/20260707-python-connector-sdk.md`` §2.1: base class
methods are declared ``async def``, but an author whose target system's
client library is sync-only (many DB drivers still are) may override with a
plain ``def`` instead. This module is the one place that detects which kind
of callable an override is and dispatches accordingly -- the same dual-mode
ergonomic FastAPI uses for path operations, "sync as a first-class option,
not a fallback hack" (§2.1).
"""

from __future__ import annotations

import asyncio
import functools
import inspect
from collections.abc import Awaitable, Callable
from typing import ParamSpec, TypeVar, cast

P = ParamSpec("P")
R = TypeVar("R")


async def invoke(func: Callable[P, Awaitable[R]], *args: P.args, **kwargs: P.kwargs) -> R:
    """Call ``func``, awaiting it if async, else running it off the event loop.

    Statically, every call site in this SDK passes an attribute of
    :class:`~conduit.source.Source`/:class:`~conduit.destination.Destination`
    (e.g. ``self._source.read``), which are declared ``async def`` on the
    ABC -- so ``func``'s declared type is always ``Callable[P,
    Awaitable[R]]`` from the type checker's point of view, even though at
    *runtime* an author may have overridden the method with a plain
    ``def`` (§2.1's dual-mode contract; a sync override is a Liskov-style
    return-type mismatch as far as static typing is concerned, but Python
    doesn't enforce that, and this function's runtime behavior handles it
    correctly regardless). ``inspect.iscoroutinefunction`` inspects the
    actual object at runtime, independent of what mypy believes its type
    is.

    A sync (plain ``def``) override runs in the default
    ``concurrent.futures.ThreadPoolExecutor`` via
    ``loop.run_in_executor`` -- never called inline on the event loop -- so a
    blocking author callable (a sync DB driver call, a blocking HTTP
    request) cannot itself wedge the loop. This is the SDK's one sanctioned
    boundary where a synchronous, potentially-blocking callable is invoked
    from async code (ruff's ``ASYNC`` lint rules flag blocking calls inside
    ``async def`` bodies elsewhere in this codebase; this function is where
    that concern is deliberately, correctly handled rather than avoided).

    Note this does not by itself fully close the hung-event-loop failure
    mode (▶ MUST-FIX 3, design doc): a sync override that blocks
    indefinitely still occupies a thread-pool worker indefinitely, and if
    every worker is exhausted, subsequent ``run_in_executor`` calls queue
    rather than run. The bounded watchdog in :mod:`conduit.serve` is the
    mitigation for a genuinely wedged process; this function's job is only
    to avoid *itself* being the thing that blocks the event loop for a
    single call.

    Args:
        func: the (possibly bound) callable to invoke -- either an
            ``async def`` or (at runtime only) a plain ``def``.
        *args: positional arguments to pass through.
        **kwargs: keyword arguments to pass through.

    Returns:
        Whatever ``func`` returns (or its awaited result, if a coroutine
        function).
    """
    if inspect.iscoroutinefunction(func):
        return await func(*args, **kwargs)
    # Reached only when `func` is, at runtime, actually a plain sync `def`
    # (an author's dual-mode override, §2.1) -- its *real* return type is
    # `R`, not `Awaitable[R]`, even though `func`'s declared static type
    # (this SDK's own `async def` ABC methods) says otherwise. This cast
    # documents that gap explicitly rather than silencing it with a bare
    # `type: ignore`.
    sync_func = cast(Callable[P, R], func)
    loop = asyncio.get_running_loop()
    bound = functools.partial(sync_func, *args, **kwargs)
    return await loop.run_in_executor(None, bound)
