"""Single source of truth for the ``conduit`` distribution version.

The ``conduit`` distribution is one semantic version shared by both
namespaces (``conduit.connector`` and ``conduit.client``), per
``docs/design/20261016-python-ga-merged-distribution.md``. Both
namespaces re-export :data:`__version__` from here so a breaking change to
either bumps the one number.

The value is a placeholder until the first tagged release; the release
workflow drives it from git tags.
"""

from __future__ import annotations

__version__ = "0.1.0.dev0"
