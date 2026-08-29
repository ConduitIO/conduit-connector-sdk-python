// Command avro_fixture_gen is the Go half of the cross-language proof behind
// conduit.schema.AvroSchema (tests/test_schema_avro.py).
//
// conduit-commons' schema/avro package (Serde) is a thin wrapper over
// github.com/iskorotkov/avro/v2 (the maintained fork of the archived
// hamba/avro/v2; adopted in conduit-commons#279) -- avro.ParseBytes plus
// avro.Marshal/avro.Unmarshal, i.e. plain, headerless Avro binary. A
// Python-only round-trip is not evidence that this SDK is wire-compatible
// with that Go codec, so tests/testdata/avro_golden.json records bytes from
// *both* encoders, and this program independently verifies, using the very
// library conduit-commons wraps:
//
//  1. that avro.Marshal reproduces each case's committed go_avro_hex
//     (the Go bytes are not invented or guessed at),
//  2. that Go decodes its own bytes to the case's expected value, and
//  3. that Go decodes each case's python_avro_hex -- bytes produced by
//     fastavro's schemaless_writer -- to that same expected value.
//
// Usage:
//
//	go run . -fixture ../../tests/testdata/avro_golden.json   # verify
//	go run . -emit -fixture ../../tests/testdata/avro_golden.json  # print avro.Marshal output per case (for regenerating go_avro_hex)
//
// Exit status is 0 only if every check passes for every case; failures are
// reported per case with the offending bytes. See the fixture's
// _provenance key and conduit/schema.py's module docstring for the full
// contract and its one documented caveat (array/map block framing:
// spec-legal both ways, mutually decodable, not byte-identical).
//
// Note on value typing: the fixture's values are JSON, which decodes to
// float64 for every number; iskorotkov/avro's Marshal rejects float64 for
// integer Avro types, so values are coerced to the schema's Go types
// (long/int -> int64, double/float -> float64, etc.) before marshaling --
// that coercion is what coerceToSchema does. Decoded values are compared to
// the expected value as canonical JSON (json.Marshal), which is
// type-identity-agnostic for integral values (int64(42) and float64(42)
// marshal identically) and exact for everything else.
package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"reflect"

	"github.com/iskorotkov/avro/v2"
)

type fixtureCase struct {
	Name                          string         `json:"name"`
	Value                         map[string]any `json:"value"`
	GoAvroHex                     string         `json:"go_avro_hex"`
	PythonAvroHex                 string         `json:"python_avro_hex"`
	ByteExactEncodeBothDirections bool           `json:"byte_exact_encode_both_directions"`
}

type fixture struct {
	SchemaJSON string        `json:"schema_json"`
	Cases      []fixtureCase `json:"cases"`
}

func main() {
	fixturePath := flag.String("fixture", "../../tests/testdata/avro_golden.json",
		"path to the golden fixture to verify (relative to this package's directory)")
	emit := flag.Bool("emit", false,
		"print avro.Marshal output per case (for regenerating go_avro_hex), then exit")
	flag.Parse()

	raw, err := os.ReadFile(*fixturePath)
	if err != nil {
		fatalf("read fixture: %v", err)
	}
	var f fixture
	if err := json.Unmarshal(raw, &f); err != nil {
		fatalf("parse fixture: %v", err)
	}
	schema, err := avro.ParseBytes([]byte(f.SchemaJSON))
	if err != nil {
		fatalf("parse schema: %v", err)
	}
	if *emit {
		for _, c := range f.Cases {
			b, err := avro.Marshal(schema, coerceToSchema(schema, c.Value))
			if err != nil {
				fatalf("%s: marshal: %v", c.Name, err)
			}
			fmt.Printf("%s %s\n", c.Name, hex.EncodeToString(b))
		}
		return
	}

	checks := 0
	failures := 0
	for _, c := range f.Cases {
		coerced := coerceToSchema(schema, c.Value)

		// 1. Go's own encoding reproduces the committed go_avro_hex.
		goBytes, err := avro.Marshal(schema, coerced)
		if err != nil {
			failures++
			fmt.Printf("FAIL %-12s marshal (go): %v\n", c.Name, err)
			continue
		}
		checks++
		if got := hex.EncodeToString(goBytes); got != c.GoAvroHex {
			failures++
			fmt.Printf("FAIL %-12s go_avro_hex differs from live avro.Marshal:\n", c.Name)
			fmt.Printf("      committed: %s\n      live:      %s\n", c.GoAvroHex, got)
		}

		// 2. Go decodes its own bytes to the expected value.
		if ok := decodeMatches(schema, goBytes, c.Value, c.Name, "go_avro_hex"); !ok {
			failures++
		}
		checks++

		// 3. Go decodes Python's bytes to the expected value.
		pythonBytes, err := hex.DecodeString(c.PythonAvroHex)
		if err != nil {
			failures++
			fmt.Printf("FAIL %-12s python_avro_hex is not valid hex: %v\n", c.Name, err)
			continue
		}
		if ok := decodeMatches(schema, pythonBytes, c.Value, c.Name, "python_avro_hex"); !ok {
			failures++
		}
		checks++
	}

	fmt.Printf("verified %d checks across %d case(s)\n", checks, len(f.Cases))
	if failures > 0 {
		fmt.Printf("%d FAILURE(S)\n", failures)
		os.Exit(1)
	}
}

// decodeMatches unmarshals b against schema and compares the result to the
// expected value canonically (both as json.Marshal output), reporting a
// mismatch with the offending bytes.
func decodeMatches(schema avro.Schema, b []byte, expected map[string]any, name, source string) bool {
	var decoded map[string]any
	if err := avro.Unmarshal(schema, b, &decoded); err != nil {
		fmt.Printf("FAIL %-12s go cannot decode %s: %v\n", name, source, err)
		return false
	}
	gotJSON, err := json.Marshal(decoded)
	if err != nil {
		fmt.Printf("FAIL %-12s marshal decoded value: %v\n", name, err)
		return false
	}
	wantJSON, err := json.Marshal(expected)
	if err != nil {
		fmt.Printf("FAIL %-12s marshal expected value: %v\n", name, err)
		return false
	}
	if !reflect.DeepEqual(gotJSON, wantJSON) {
		fmt.Printf("FAIL %-12s %s decodes to a different value:\n", name, source)
		fmt.Printf("      decoded: %s\n      expected: %s\n", gotJSON, wantJSON)
		return false
	}
	return true
}

// coerceToSchema converts a JSON-decoded value (all numbers float64) into
// the Go types iskorotkov/avro's Marshal expects for the given schema:
// integral types to int64, double/float to float64, arrays to []any, maps to
// map[string]any, and union members to the first matching branch. This
// mirrors what conduit-commons' engine does when it marshals
// opencdc.StructuredData, and it is what the "Go side" of the fixture's
// value would naturally be.
func coerceToSchema(schema avro.Schema, v any) any {
	if v == nil {
		return nil
	}
	switch schema.Type() {
	case avro.Record:
		rec, ok := v.(map[string]any)
		if !ok {
			return v
		}
		out := make(map[string]any, len(rec))
		for _, field := range schema.(*avro.RecordSchema).Fields() {
			if fv, present := rec[field.Name()]; present {
				out[field.Name()] = coerceToSchema(field.Type(), fv)
			}
		}
		return out
	case avro.Array:
		arr, ok := v.([]any)
		if !ok {
			return v
		}
		items := schema.(*avro.ArraySchema).Items()
		out := make([]any, len(arr))
		for i, e := range arr {
			out[i] = coerceToSchema(items, e)
		}
		return out
	case avro.Map:
		m, ok := v.(map[string]any)
		if !ok {
			return v
		}
		values := schema.(*avro.MapSchema).Values()
		out := make(map[string]any, len(m))
		for k, e := range m {
			out[k] = coerceToSchema(values, e)
		}
		return out
	case avro.Union:
		for _, member := range schema.(*avro.UnionSchema).Types() {
			if member.Type() == avro.Null {
				if v == nil {
					return nil
				}
				continue
			}
			if member.Type() == avro.Record {
				// Named-type union member; only match when the value is a
				// non-nil map (avro.Marshal would reject it otherwise).
				if _, ok := v.(map[string]any); ok {
					return coerceToSchema(member, v)
				}
				continue
			}
			if coerceable := coerceToSchema(member, v); isCoerced(v, coerceable) {
				return coerceable
			}
		}
		return v
	case avro.Long, avro.Int:
		switch n := v.(type) {
		case float64:
			if n != float64(int64(n)) {
				return v // non-integral; let avro.Marshal report the error
			}
			return int64(n)
		case json.Number:
			i, err := n.Int64()
			if err == nil {
				return i
			}
			return v
		}
		return v
	case avro.Double, avro.Float:
		switch n := v.(type) {
		case int64:
			return float64(n)
		case json.Number:
			f, err := n.Float64()
			if err == nil {
				return f
			}
			return v
		}
		return v
	default:
		return v // string, boolean, null, enum, fixed, bytes: pass through
	}
}

// isCoerced reports whether coerceToSchema actually transformed v (used to
// pick the first union branch whose coercion "fits" the value).
func isCoerced(before, after any) bool {
	if before == nil || after == nil {
		return before == after
	}
	return reflect.TypeOf(before) != reflect.TypeOf(after)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "avro_fixture_gen: "+format+"\n", args...)
	os.Exit(2)
}
