"""``conduit-connector-sdk`` console-script entry point.

Currently one subcommand, ``build`` (see :mod:`conduit._build`). Mirrors the
org-wide convention (Conduit's own Go CLI) of a single top-level command
with subcommands rather than a proliferation of separate scripts.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from conduit._build import BuildError, build_connector_artifact


def main(argv: list[str] | None = None) -> int:
    """Entry point for the ``conduit-connector-sdk`` console script.

    Args:
        argv: argument vector, excluding the program name; defaults to
            ``sys.argv[1:]``. Injectable for testing.

    Returns:
        Process exit code (``0`` on success).
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "build":
        try:
            output = build_connector_artifact(
                args.project_dir,
                args.output,
                entry_point=args.entry_point,
                interpreter=args.interpreter,
            )
        except BuildError as exc:
            print(f"conduit-connector-sdk build: error: {exc}", file=sys.stderr)
            return 1
        print(f"built {output}")
        return 0

    parser.print_help(sys.stderr)
    return 1


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="conduit-connector-sdk")
    subparsers = parser.add_subparsers(dest="command")

    build_parser = subparsers.add_parser(
        "build",
        help=(
            "Build a self-contained, directly-executable connector artifact "
            "(a zipapp with an absolute-interpreter-path shebang, bundling "
            "every third-party dependency). Required for Conduit to launch "
            "a standalone connector -- see "
            "docs/design/20260707-python-connector-sdk.md §1.1.6."
        ),
    )
    build_parser.add_argument(
        "project_dir", type=Path, help="path to the connector project directory"
    )
    build_parser.add_argument(
        "-o", "--output", type=Path, required=True, help="output artifact path"
    )
    build_parser.add_argument(
        "--entry-point",
        default="main.py",
        help="entry script within project_dir (default: main.py)",
    )
    build_parser.add_argument(
        "--interpreter",
        default=None,
        help=(
            "absolute interpreter path to embed in the artifact's shebang "
            "(default: the interpreter running this build command)"
        ),
    )
    return parser


if __name__ == "__main__":
    sys.exit(main())
