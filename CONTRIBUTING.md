# Contributing

Thanks for considering a contribution to `conduit-connector-sdk-python`.

## Tier and review bar

This repo is **Tier 1 (data path)** in the ConduitIO org's review taxonomy
(see [`ConduitIO/conduit`'s `CLAUDE.md`](https://github.com/ConduitIO/conduit/blob/main/CLAUDE.md)):
it originates and acknowledges records on one side of the exact gRPC boundary
Conduit's engine treats as authoritative. Concretely:

- Any change touching the wire adapter (`_grpc/`, `_handshake.py`, `serve.py`),
  the ack/nack logic in `destination.py`/`source.py`, position handling, or the
  `Record`/`Config` codec requires a design doc or an explicit waiver for small
  fixes, per `docs/design/`.
- Human maintainer sign-off is required on Tier-1 changes; automated review
  alone is never sufficient.
- Bug fixes ship with the regression test that would have caught the bug.
- PR descriptions include a failure-mode analysis: what could this break, what
  would show it, how do we roll back.

## Local setup

```bash
uv sync --all-extras
# or: python3 -m venv .venv && . .venv/bin/activate && pip install -e '.[dev]'
```

## Before opening a PR

```bash
ruff format --check .
ruff check .
mypy
pytest
```

Regenerating the gRPC stubs (only needed when `conduit-connector-protocol` or
the `opencdc`/`config` messages in `conduit-commons` change):

```bash
./tools/generate-stubs.sh
```

Review the diff under `src/conduit/_grpc/` before committing — this is the
one directory in the repo that's generated, vendored output (see
`src/conduit/_grpc/__init__.py` for why it's structured the way it is).

## Commit style

Conventional commits (`feat:`, `fix:`, `docs:`, `refactor:`, `chore:`), matching
the rest of the ConduitIO org.
