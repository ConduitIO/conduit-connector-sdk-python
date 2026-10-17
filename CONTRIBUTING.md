# Contributing

Thanks for considering a contribution to the `conduit` Python distribution
(published as `conduitio`). This repo holds two namespaces under the one
import package: `conduit.connector` (the connector SDK) and `conduit.client`
(the gRPC control client).

## Tier and review bar

The **`conduit.connector`** namespace is **Tier 1 (data path)** in the
ConduitIO org's review taxonomy (see [`ConduitIO/conduit`'s
`CLAUDE.md`](https://github.com/ConduitIO/conduit/blob/main/CLAUDE.md)):
it originates and acknowledges records on one side of the exact gRPC boundary
Conduit's engine treats as authoritative. Concretely:

- Any change touching the wire adapter (`_grpc/`, `connector/_handshake.py`,
  `connector/serve.py`), the ack/nack logic in `connector/destination.py`/
  `connector/source.py`, position handling, or the `Record`/`Config` codec
  requires a design doc or an explicit waiver for small fixes, per
  `docs/design/`.
- Human maintainer sign-off is required on Tier-1 changes; automated review
  alone is never sufficient.
- Bug fixes ship with the regression test that would have caught the bug.
- PR descriptions include a failure-mode analysis: what could this break, what
  would show it, how do we roll back.

The **`conduit.client`** namespace is Tier 2 (feature): the control client
drives lifecycle/status RPCs over gRPC, not the record path.

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

Regenerating the gRPC stubs (only needed when `conduit-connector-protocol`,
the `conduit-commons` `opencdc`/`config` messages, or the control-plane API
in `ConduitIO/conduit` change):

```bash
./tools/generate-stubs.sh
```

Review the diff under `src/conduit/_grpc/` before committing — this is the
one directory in the repo that's generated, vendored output (see
`src/conduit/_grpc/__init__.py` for why it's structured the way it is).

## Commit style

Conventional commits (`feat:`, `fix:`, `docs:`, `refactor:`, `chore:`), matching
the rest of the ConduitIO org.
