# Contributing

Thanks for considering a contribution to `conduit-client-python`.

## Tier and review bar

Per [`ConduitIO/conduit`'s `CLAUDE.md`](https://github.com/ConduitIO/conduit/blob/main/CLAUDE.md),
this is a new public client-facing API surface (Tier 1 territory per the ADR
that authorized it, `docs/design/20260724-embed-bindings-via-grpc.md`):

- Any change to the builder's payload shape (`pipeline.py`), the gRPC wire
  adapter (`_grpc/`, `client.py`), error translation (`errors.py`), or binary
  provisioning (`_provision.py`, `_local.py`) is a public-API-shape change --
  flag it loudly in the PR description; the builder/`local()`/`connect()`
  surface is meant to be frozen once it ships.
- Human maintainer sign-off is required on changes to that surface --
  automated review alone is never sufficient.
- Bug fixes ship with the regression test that would have caught the bug.
- PR descriptions include a failure-mode analysis: what could this break,
  what would show it, how do we roll back.

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
pytest -v -m "not integration"   # fast, no network, no subprocess
pytest -v -m integration         # real conduit binary end-to-end (needs network)
```

Regenerating the gRPC stubs (only needed when `proto/api/v1/api.proto` or its
dependencies change):

```bash
./tools/generate-stubs.sh
```

Review the diff under `src/conduit/_grpc/` before committing -- this is the
one directory in the repo that's generated, vendored output (see
`src/conduit/_grpc/__init__.py` for why it's structured the way it is).

## Commit style

Conventional commits (`feat:`, `fix:`, `docs:`, `refactor:`, `chore:`),
matching the rest of the ConduitIO org.
