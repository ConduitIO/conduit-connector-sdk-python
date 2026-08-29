# avro_fixture_gen — cross-language Avro wire-compatibility verifier

The Go half of the proof that `conduit.schema.AvroSchema`
(`tests/test_schema_avro.py`) is wire-compatible with `conduit-commons`'
`schema/avro` package. It uses `github.com/iskorotkov/avro/v2` directly —
the exact library `conduit-commons` wraps (`schema/avro/serde.go`:
`avro.ParseBytes` + `avro.Marshal`/`avro.Unmarshal`; the fork of the
archived `hamba/avro/v2`, adopted in conduit-commons#279).

## What it verifies

For every case in `tests/testdata/avro_golden.json`:

1. `avro.Marshal(schema, value)` reproduces the committed `go_avro_hex` —
   the Go bytes cannot silently drift from the real codec.
2. Go decodes `go_avro_hex` to the expected value.
3. Go decodes `python_avro_hex` (this SDK's `encode()` output, pinned by
   the Python tests) to the expected value.

Values are JSON (all numbers decode as `float64`); they are coerced to the
Go types `avro.Marshal` expects via `coerceToSchema` (long/int → `int64`,
double/float → `float64`, array → `[]any`, map → `map[string]any`), and
decoded values are compared to expectations as canonical JSON
(`json.Marshal`), which is type-identity-agnostic for integral values.

## Usage

```sh
# Verify the committed fixture (exit 0 = all checks pass).
go run . -fixture ../../tests/testdata/avro_golden.json

# Regenerate go_avro_hex for every case (prints "<name> <hex>" lines).
go run . -emit -fixture ../../tests/testdata/avro_golden.json
```

Regenerating `python_avro_hex` requires the Python side: encode each
case's value with `conduit.schema.AvroSchema.encode` (or
`fastavro.schemaless_writer` directly) and paste the hex into the fixture,
then run the verifier before committing. The fixture's `_provenance` key
documents the exact codecs and the two honest caveats (array block
framing: spec-legal both ways, mutually decodable, not byte-identical;
map fields: nondeterministic on the Go side by construction — Go map
iteration order — so the fixture deliberately has no map field).

This tool is also run by `tests/test_schema_avro.py::test_go_verifier_confirms_python_bytes_decode_in_go`
whenever a Go toolchain is available; Python-only CI relies on the
committed fixture, which the verify mode keeps honest.
