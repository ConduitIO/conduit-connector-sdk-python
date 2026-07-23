# http-poll-source (not started)

The Phase-1 worked example connector from
`docs/design/20260707-python-connector-sdk.md` §2.7 — a minimal source that
polls an HTTP endpoint for new rows. This is **Lane D** in the v0.19
workstream, depending on Lane B (`Source`/`Destination` ABCs, `BaseConfig`,
the OpenCDC record model) — none of which exists yet as of this scaffold.

This will become both the worked example *and* the source for
`conduit connector new --lang python`'s scaffolded template (Lane E), per the
design doc's §11 reasoning for reusing one connector for both rather than
building a separate template repo before the SDK API has stabilized.
