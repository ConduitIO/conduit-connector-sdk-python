"""Avro schema support, wire-compatible with ``conduit-commons``' ``schema/avro`` package.

**What this module is:** a thin wrapper over `fastavro
<https://fastavro.readthedocs.io/>`_'s schemaless reader/writer --
:class:`AvroSchema` -- that produces and consumes the *exact same wire
bytes* Conduit's Go side does for Avro-typed record payloads. This is a
cross-language claim (``conduit-commons`` is Go), so it is backed by a
cross-language proof, not a Python-only round-trip: see
``tests/test_schema_avro.py``, ``tests/testdata/avro_golden.json``, and the
Go verifier in ``tools/avro_fixture_gen``.

**The wire format, precisely:** ``conduit-commons``' ``schema/avro.Serde``
(``schema/avro/serde.go``) wraps `github.com/iskorotkov/avro/v2
<https://github.com/iskorotkov/avro>`_ directly -- ``avro.Marshal(schema, v)``/
``avro.Unmarshal(schema, b, &v)``, i.e. **plain Avro binary encoding**
(what Avro itself calls "schemaless"), with no header of any kind. (The
fork replaced the archived ``hamba/avro/v2`` module in
``conduit-commons#279``; it is a fork of hamba, so the wire format is
unchanged.) This is *not* the Confluent Schema Registry wire format (no
leading magic byte + 4-byte schema ID) -- Conduit's own schema reference
lives in the record's ``opencdc.{key,payload}.schema.{subject,version}``
metadata instead (see :meth:`conduit.record.Metadata.set_payload_schema`/
:meth:`~conduit.record.Metadata.get_payload_schema`), not embedded in the
encoded bytes themselves. :func:`fastavro.schemaless_writer`/
:func:`fastavro.schemaless_reader` produce/consume exactly that same
headerless format, which is what makes byte-level wire compatibility
possible here at all -- a Confluent-wire-format library would not be
compatible no matter how the schema itself matched.

**Cross-language proof, and its honest limits:**
``tests/testdata/avro_golden.json`` records, for each case, bytes from
*both* encoders: ``go_avro_hex`` (what ``avro.Marshal`` produces today,
re-derived live and compared by the Go verifier, so the committed bytes
cannot silently drift from the real codec) and ``python_avro_hex`` (what
this module's ``encode`` produces, pinned byte-exactly by the Python
tests). ``tools/avro_fixture_gen``'s verify mode independently confirms --
using the very library ``conduit-commons`` wraps -- that Go decodes the
Python bytes to the expected value, and the Python tests confirm the
reverse direction. Two documented caveats:

- **Array block framing.** Avro permits two spec-legal framing styles per
  block (a plain positive item count, or a negative count paired with an
  explicit byte-size prefix for skip-ahead support) -- the Go codec chooses
  the size-prefixed form, fastavro the plain form. Both are spec-compliant
  and mutually decodable (verified in both directions -- see the golden
  fixture), but not byte-identical for schemas containing a non-empty
  array field. Scalar-only records (no non-empty array field) round-trip
  **byte-for-byte** in both directions; the ``tags`` array case in the
  golden fixture is the one that isn't byte-identical on encode,
  documented at that exact assertion in the test.
- **Map fields are nondeterministic on the Go side, by construction.**
  Go's ``avro.Marshal`` iterates map entries in randomized Go-map order, so
  a record with a non-empty ``map`` field cannot have stable Go-produced
  bytes at all -- byte-compat across languages is impossible there no
  matter what a Python encoder does (decoding works regardless; this is a
  Go-side property, not a Python deficiency). The golden fixture therefore
  has no map field; see its ``map_nondeterminism_note``.

**What this module does not do (an honest scope boundary, not an
oversight):**

- **No schema registry client.** There is no ``SchemaService`` gRPC
  client in this SDK (no generated stubs for it either -- see
  ``buf.gen.yaml``, scoped deliberately to ``connector/v2`` +
  ``opencdc/v1`` + ``config/v1``). A connector author supplies the schema
  text themselves -- e.g. a schema authored alongside the connector, or
  fetched by whatever means their own code chooses -- and is responsible
  for keeping the ``opencdc.*.schema.subject``/``.version`` metadata (see
  :mod:`conduit.record`) consistent with it. Wiring a real registry
  client through is future work, not part of what "Avro/schema support"
  means in this module.
- **No schema evolution/compatibility checking.** :class:`AvroSchema` is a
  parse-and-(de)serialize wrapper, not a compatibility-checking layer;
  reader/writer schema resolution beyond what ``fastavro`` itself performs
  is out of scope here.
- **No automatic extraction middleware.** The Go SDK's
  ``SourceWithSchemaExtraction``/``DestinationWithSchemaExtraction``
  (register a schema, encode/decode records, attach subject/version
  metadata to records) is not reimplemented: there is no registry to
  register against (see above), so an author calls :meth:`AvroSchema.encode`
  and :meth:`conduit.record.Metadata.set_payload_schema` explicitly.
"""

from __future__ import annotations

import io
import json
from collections.abc import Mapping
from typing import Any, cast

import fastavro
from fastavro.schema import SchemaParseException


class AvroSchema:
    """A parsed Avro schema, ready to encode/decode values against.

    Mirrors ``conduit-commons``' ``schema/avro.Serde`` shape
    (``Parse``/``Marshal``/``Unmarshal``) closely enough to be recognizable
    to someone coming from the Go SDK, without pretending to be a literal
    port -- see the module docstring for the exact wire contract this
    implements and its proven/limits.
    """

    __slots__ = ("_parsed", "_text")

    def __init__(self, text: str) -> None:
        """Parse ``text`` (an Avro schema in JSON form) immediately.

        Args:
            text: the Avro schema, as JSON text -- the same bytes
                ``conduit-commons``' ``schema.Schema.Bytes`` field carries.

        Raises:
            ValueError: if ``text`` is not valid JSON or not a valid Avro
                schema (``fastavro.parse_schema`` rejects it).
        """
        self._text = text
        try:
            self._parsed = fastavro.parse_schema(json.loads(text))
        except (ValueError, SchemaParseException) as exc:
            # `ValueError` covers both `json.JSONDecodeError` (malformed
            # JSON) and fastavro's own `UnknownType`/similar schema-shape
            # errors, which -- inconsistently, on fastavro's side, not
            # ours -- derive from `ValueError` directly rather than from
            # `SchemaParseException` (caught explicitly alongside it since
            # it does *not* derive from `ValueError`). Both are normalized
            # to one exception type here so callers have a single,
            # predictable exception to catch regardless of which internal
            # fastavro error path a given malformed schema hits.
            raise ValueError(f"invalid Avro schema: {exc}") from exc

    @property
    def text(self) -> str:
        """The original schema text this instance was parsed from."""
        return self._text

    def encode(self, value: Mapping[str, Any]) -> bytes:
        """Encode ``value`` as plain (headerless) Avro binary.

        Args:
            value: a JSON-like mapping matching this schema's fields.

        Returns:
            The Avro-encoded bytes -- wire-compatible with
            ``conduit-commons``' ``schema.Schema.Marshal`` for
            scalar/string/boolean/numeric/union fields; see the module
            docstring's array/map framing caveat for the one documented
            exception.

        Raises:
            TypeError: if a field's value has the wrong Python type for its
                Avro type.
            ValueError: if a required field is missing or has no default.
            OverflowError: if an integer is out of range for its Avro type.
            The exact exception is fastavro's choice -- the contract is
            that every mismatch fails loudly at encode time, never silent
            coercion or a dropped record (invariant 6).
        """
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, self._parsed, value)
        return buf.getvalue()

    def decode(self, data: bytes) -> dict[str, Any]:
        """Decode plain (headerless) Avro binary produced against this schema.

        Args:
            data: Avro-encoded bytes -- from this class's own
                :meth:`encode`, or from a Go-side
                ``schema.Schema.Marshal`` (``iskorotkov/avro``) call using
                the same schema text. Both are accepted identically; this
                is the direction the module docstring's cross-language
                proof is strongest on (byte-exact for every field type
                Avro's spec allows more than one legal encoding of).

        Returns:
            The decoded value as a plain ``dict``.

        Raises:
            TypeError: if this schema isn't (or doesn't resolve to) an
                Avro ``record`` -- ``fastavro`` can technically decode any
                Avro type, but ``AvroSchema`` is documented (and typed) as
                a record-shaped, dict-in/dict-out wrapper, matching what
                every actual OpenCDC payload use looks like.
            EOFError: if ``data`` is truncated (fewer bytes than the
                schema requires).
            ValueError/IndexError: for other malformed encodings
                (fastavro's choice of exception; explicit either way).
                Decoding failures are always explicit errors, never silent
                coercion or dropped data (invariant 6).
        """
        decoded = fastavro.schemaless_reader(io.BytesIO(data), self._parsed)
        if not isinstance(decoded, dict):
            raise TypeError(
                f"AvroSchema.decode: schema did not decode to a record (dict), "
                f"got {type(decoded).__name__} -- AvroSchema only supports record schemas"
            )
        return cast(dict[str, Any], decoded)

    @classmethod
    def parse(cls, text: str) -> AvroSchema:
        """Alternate constructor, matching Go's ``avro.Parse(text)`` naming.

        Equivalent to ``AvroSchema(text)`` -- provided so code translating
        from the Go SDK's ``schema.Parse(bytes)``/``avro.Parse(text)`` call
        shape has an exact-name match to reach for.
        """
        return cls(text)


__all__ = ["AvroSchema"]
