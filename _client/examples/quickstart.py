"""The README/design-doc quickstart, runnable as-is (``python examples/quickstart.py``).

Downloads a pinned `conduit` binary on first use (see
`conduit._provision.DEFAULT_CONDUIT_VERSION`), spawns it, creates and runs a
`generator -> log` pipeline, and gracefully stops it. This exact block --
minus this module docstring -- is the ≤15-line quickstart quoted in
`README.md` and `src/conduit/__init__.py`.
"""

import conduit

pipeline = (
    conduit.Pipeline("orders-sync")
    .source("generator", settings={"format.type": "structured"}, operations="create")
    .destination("log", level="info")
)

with conduit.local(state_dir="./conduit-state") as client:
    run = client.run(pipeline)
    run.wait_running()
    print(run.status())
    run.stop()
