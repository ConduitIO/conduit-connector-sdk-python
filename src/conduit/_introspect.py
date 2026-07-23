"""Shared generic-parameter introspection for ``Source``/``Destination``.

Both ``Source[ConfigT]`` and ``Destination[ConfigT]`` need to recover the
concrete ``ConfigT`` an author parameterized their subclass with (e.g.
``class HTTPPollSource(Source[Config])``) so the SDK can validate the
``Configure`` RPC's config map against it and introspect it (via
:func:`conduit.config.to_parameters`) for the ``Specify`` RPC -- without the
author writing any boilerplate ``config_class = Config`` class attribute.
This one small helper is shared by both modules rather than duplicated.
"""

from __future__ import annotations

import typing


def resolve_config_class(cls: type, base: type) -> type:
    """Find the concrete type argument a subclass parameterized ``base`` with.

    Walks ``cls.__mro__`` looking for a class in the chain whose
    ``__orig_bases__`` includes a parameterized generic alias of ``base``
    (e.g. ``Source[Config]``), and returns that type argument.

    Args:
        cls: the concrete ``Source``/``Destination`` subclass an author
            wrote, e.g. ``HTTPPollSource``.
        base: the generic base class to look for, e.g. ``Source``.

    Returns:
        The concrete config class, e.g. ``Config``.

    Raises:
        TypeError: if no ancestor in ``cls``'s MRO parameterizes ``base``
            with a concrete type -- i.e. the author wrote
            ``class Foo(Source):`` instead of ``class Foo(Source[Config]):``.
    """
    for klass in cls.__mro__:
        for orig_base in getattr(klass, "__orig_bases__", ()):
            if typing.get_origin(orig_base) is base:
                args = typing.get_args(orig_base)
                if args and isinstance(args[0], type):
                    return args[0]
    raise TypeError(
        f"{cls.__name__} must parameterize {base.__name__} with a concrete "
        f"config class, e.g. `class {cls.__name__}({base.__name__}[YourConfig]):"
        " ...` -- the SDK introspects this to validate the `Configure` RPC's "
        "config map and to build the `Specify` RPC's parameter map "
        "(see docs/design/20260707-python-connector-sdk.md §2.2/§2.4)."
    )
