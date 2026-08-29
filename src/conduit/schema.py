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
reverse direction. Three documented caveats:

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
  a record with a non-empty ``map`` field has *no* stable Go-produced
  bytes at all -- byte-compat across languages is impossible there no
  matter what a Python encoder does. The golden fixture's ``with_map``
  case therefore pins only ``python_avro_hex`` (fastavro iterates dicts in
  insertion order, so Python bytes are stable); the Go verifier still
  decodes those Python bytes, and the live-Go test decodes Go's own
  freshly-marshaled map bytes in Python -- decoding is order-independent
  in both codecs, which is the property that actually matters (this is a
  Go-side property, not a Python deficiency).
- **Bytes fields in the fixture.** JSON has no bytes literal, so the
  fixture's ``value`` holds a bytes-typed field's content as a UTF-8
  string and both codecs convert str<->bytes at the field level (listed in
  the case's ``bytes_fields``); the pinned hexes cover the encoded form.

**Encode-side validation (never silent coercion).** :meth:`AvroSchema.encode`
validates the value against the parsed schema *before* writing any bytes,
with the same strictness the Go codec applies at marshal time: ``int``/
``long`` fields require Python ``int`` (``bool`` is rejected even though
it subclasses ``int``), ``float``/``double`` fields require ``float``,
``string``/``bytes``/``boolean`` require their exact types, and a key the
schema has no field for is rejected. fastavro by itself silently coerces
some of these (``42.9`` into a ``long`` writes ``42``; ``True`` into a
``long`` writes ``1``; extra dict keys are dropped) -- exactly the
invariant-6 violation this validation exists to prevent: a connector whose
data drifts type-wise must fail loudly at encode time, never write a
*different value* downstream. One deliberate divergence from the Go codec
is part of this contract: Go's ``avro.Marshal`` silently *drops* record
keys the schema has no field for, and this SDK rejects them with
``TypeError`` instead (silent truncation is never acceptable under
invariant 6; a typo'd or schema-drifted key is a loud error here).

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

import importlib
import io
import json
from collections.abc import Mapping
from typing import Any, cast

__all__ = ["AvroSchema"]

# fastavro lives behind the optional `[avro]` extra, so it is imported
# lazily on first use rather than at module import time: `import
# conduit.schema` (and `from conduit.schema import AvroSchema`) must work
# without it, and the first actual use must fail with an install hint, not
# a bare "No module named 'fastavro'".
_fastavro_module: Any = None


def _fastavro() -> Any:
    """Import fastavro on first use; raise a helpful error if it's missing."""
    global _fastavro_module
    if _fastavro_module is None:
        try:
            _fastavro_module = importlib.import_module("fastavro")
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "AvroSchema requires the optional 'fastavro' dependency -- "
                "install conduit-connector-sdk with the 'avro' extra: "
                "`pip install 'conduit-connector-sdk[avro]'`"
            ) from exc
    return _fastavro_module


_AVRO_PRIMITIVES = frozenset(
    {"null", "boolean", "int", "long", "float", "double", "bytes", "string"}
)
_INT_MIN, _INT_MAX = -(2**31), 2**31 - 1
_LONG_MIN, _LONG_MAX = -(2**63), 2**63 - 1


def _resolve_named(name: str, named: dict[str, Any]) -> Any:
    """Resolve a named-type reference against fastavro's ``__named_schemas``.

    fastavro emits references as bare names (e.g. ``"B"``) while its
    registry keys are full names (``"io.conduit.example.B"``), so a suffix
    match is needed when the bare name isn't already a key.
    """
    if name in named:
        return named[name]
    for full_name, resolved in named.items():
        if full_name.endswith("." + name):
            return resolved
    return None


def _validate_value(schema: Any, value: Any, path: str, named: dict[str, Any]) -> None:
    """Validate ``value`` against a fastavro-parsed ``schema``, strictly.

    Raises TypeError/ValueError/OverflowError on any mismatch -- the same
    exception classes fastavro's own writer raises for the mismatches it
    does catch, extended here to the *silent* coercions it (and the Go
    codec's mirror checks) would otherwise let through: float into a long,
    bool into a long, int into a double, unknown record keys, and friends.
    ``path`` names the value's location (``"id"``, ``"inner.x"``,
    ``"tags[2]"``, ``"attrs<k>"``) for actionable errors.
    """
    if isinstance(schema, list):  # union
        for branch in schema:
            try:
                _validate_value(branch, value, path, named)
                return
            except (TypeError, ValueError, OverflowError):
                continue
        branch_names = [b.get("type") if isinstance(b, dict) else b for b in schema]
        raise TypeError(
            f'encode: field "{path}": {type(value).__name__} matches no branch '
            f"of union {branch_names}"
        )
    if isinstance(schema, dict):
        schema_type = schema.get("type")
        if schema_type == "record":
            _validate_record(schema, value, path, named)
            return
        if schema_type == "array":
            if not isinstance(value, (list, tuple)):
                raise TypeError(
                    f'encode: field "{path}": expected an array (list), got {type(value).__name__}'
                )
            items = schema["items"]
            for index, item in enumerate(value):
                _validate_value(items, item, f"{path}[{index}]", named)
            return
        if schema_type == "map":
            if not isinstance(value, dict):
                raise TypeError(
                    f'encode: field "{path}": expected a map (dict), got {type(value).__name__}'
                )
            values = schema["values"]
            for key, item in value.items():
                if type(key) is not str:
                    raise TypeError(
                        f'encode: field "{path}": map keys must be strings, got '
                        f"{type(key).__name__}"
                    )
                _validate_value(values, item, f"{path}<{key}>", named)
            return
        if schema_type == "enum":
            if type(value) is not str:
                raise TypeError(
                    f'encode: field "{path}": expected a string enum symbol, got '
                    f"{type(value).__name__}"
                )
            if value not in schema["symbols"]:
                raise ValueError(
                    f'encode: field "{path}": {value!r} is not in enum symbols {schema["symbols"]}'
                )
            return
        if schema_type == "fixed":
            if type(value) is not bytes:
                raise TypeError(
                    f'encode: field "{path}": expected bytes of length '
                    f"{schema['size']}, got {type(value).__name__}"
                )
            if len(value) != schema["size"]:
                raise ValueError(
                    f'encode: field "{path}": {len(value)} bytes do not match '
                    f"fixed size {schema['size']}"
                )
            return
        if isinstance(schema_type, str) and schema_type in _AVRO_PRIMITIVES:
            # Includes logical types: fastavro keeps them as
            # {"type": <primitive>, "logicalType": ...}, so the underlying
            # primitive's check is the right one.
            _validate_primitive(schema_type, value, path)
            return
        if isinstance(schema_type, str):
            # A named-type reference written as {"type": "B"} (fastavro
            # usually emits bare "B" instead, handled below).
            resolved = _resolve_named(schema_type, named)
            if resolved is None:
                raise TypeError(
                    f'encode: field "{path}": cannot resolve schema reference {schema_type!r}'
                )
            _validate_value(resolved, value, path, named)
            return
        raise TypeError(f'encode: field "{path}": unsupported schema {schema!r}')
    if isinstance(schema, str):
        if schema in _AVRO_PRIMITIVES:
            _validate_primitive(schema, value, path)
            return
        resolved = _resolve_named(schema, named)
        if resolved is None:
            raise TypeError(f'encode: field "{path}": cannot resolve schema reference {schema!r}')
        _validate_value(resolved, value, path, named)
        return
    raise TypeError(f'encode: field "{path}": cannot validate against schema {schema!r}')


def _validate_record(schema: dict[str, Any], value: Any, path: str, named: dict[str, Any]) -> None:
    if not isinstance(value, dict):
        raise TypeError(
            f'encode: field "{path}": expected a record (dict), got {type(value).__name__}'
        )
    record_name = schema.get("name", path)
    fields = schema["fields"]
    known = {field["name"] for field in fields}
    for key in value:
        if key not in known:
            # Deliberate divergence from the Go codec, which silently drops
            # unknown record keys: silent truncation is never acceptable
            # under invariant 6 (see the module docstring).
            raise TypeError(
                f'encode: unknown field "{key}" in record "{record_name}" '
                f"(the schema has no such field; rejected rather than silently dropped)"
            )
    for field in fields:
        name = field["name"]
        if name in value:
            field_path = f"{path}.{name}" if path else name
            _validate_value(field["type"], value[name], field_path, named)
        elif "default" not in field:
            # Both codecs write a field's default when the value omits it
            # (verified live against the Go codec) -- but a field with no
            # default must be present: Go errors "missing required field",
            # fastavro errors, and so do we, before anything is written.
            raise ValueError(
                f'encode: missing required field "{name}" in record "{record_name}" (no default)'
            )


def _validate_primitive(avro_type: str, value: Any, path: str) -> None:
    """Type-check ``value`` against a primitive Avro type, with Go-codec strictness.

    ``type(v) is`` (not ``isinstance``) everywhere: ``bool`` subclasses
    ``int`` and would otherwise silently pass for ``int``/``long`` fields.
    """
    if avro_type in ("int", "long"):
        if type(value) is not int:
            raise TypeError(
                f'encode: field "{path}": {type(value).__name__} is unsupported '
                f"for Avro {avro_type}"
            )
        low, high = (_INT_MIN, _INT_MAX) if avro_type == "int" else (_LONG_MIN, _LONG_MAX)
        if not low <= value <= high:
            raise OverflowError(
                f'encode: field "{path}": {value} is out of range for Avro {avro_type}'
            )
        return
    if avro_type in ("float", "double"):
        if type(value) is not float:
            raise TypeError(
                f'encode: field "{path}": {type(value).__name__} is unsupported '
                f"for Avro {avro_type}"
            )
        return
    if avro_type == "boolean":
        if type(value) is not bool:
            raise TypeError(
                f'encode: field "{path}": {type(value).__name__} is unsupported for Avro boolean'
            )
        return
    if avro_type == "string":
        if type(value) is not str:
            raise TypeError(
                f'encode: field "{path}": {type(value).__name__} is unsupported for Avro string'
            )
        return
    if avro_type == "bytes":
        if type(value) is not bytes:
            raise TypeError(
                f'encode: field "{path}": {type(value).__name__} is unsupported for Avro bytes'
            )
        return
    if avro_type == "null":
        if value is not None:
            raise TypeError(f'encode: field "{path}": expected null, got {type(value).__name__}')


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
                schema. fastavro raises several different exception classes
                for malformed schemas depending on the malformation
                (``ValueError``/``UnknownType``, ``SchemaParseException``,
                ``KeyError`` for a dict with no ``type``, ``AttributeError``
                for a ``fields`` value that isn't a list, ``TypeError`` for
                a union written as a top-level ``type``, ...); all are
                normalized to this single, predictable exception.
            ModuleNotFoundError: if fastavro is not installed (install with
                ``pip install 'conduit-connector-sdk[avro]'``).
        """
        self._text = text
        fastavro = _fastavro()  # raises the [avro]-extra hint if fastavro is missing
        try:
            self._parsed = fastavro.parse_schema(json.loads(text))
        except Exception as exc:
            # SchemaParseException does not derive from ValueError, and
            # fastavro's other parse failures span KeyError/AttributeError/
            # TypeError -- a tuple catch would need the lazily-imported
            # class, so normalize every parser failure to ValueError.
            raise ValueError(f"invalid Avro schema: {exc}") from exc

    @property
    def text(self) -> str:
        """The original schema text this instance was parsed from."""
        return self._text

    def encode(self, value: Mapping[str, Any]) -> bytes:
        """Encode ``value`` as plain (headerless) Avro binary.

        ``value`` is validated against the schema *before* any bytes are
        written, with the same strictness the Go codec conduit-commons
        wraps applies at marshal time: a value that would be silently
        coerced or truncated -- ``42.9`` into a ``long``, ``True`` into a
        ``long``, an int into a ``double``, a key the schema has no field
        for -- is an error, never a *different value* on the wire
        (invariant 6). One deliberate divergence from Go: it silently
        drops unknown record keys, this SDK rejects them (see the module
        docstring).

        Args:
            value: a plain ``dict`` matching this schema's fields. A field
                missing from ``value`` must have a ``default`` in the
                schema -- the default is written, exactly as the Go codec
                does; a missing field with no default is an error.

        Returns:
            The Avro-encoded bytes -- wire-compatible with
            ``conduit-commons``' ``schema.Schema.Marshal`` for every field
            type the fixture pins (see the module docstring's array/map
            framing caveat for the one documented exception).

        Raises:
            TypeError: if a field's value has the wrong Python type for
                its Avro type (``int``/``long`` require ``int`` -- ``bool``
                is rejected; ``float``/``double`` require ``float``;
                ``string``/``bytes``/``boolean`` require their exact
                types), or if ``value`` has a key the schema has no field
                for.
            ValueError: if a required field (no default) is missing from
                ``value``, or a value is invalid for an ``enum``/``fixed``
                field.
            OverflowError: if an int is outside its Avro type's range.
            ModuleNotFoundError: if fastavro is not installed (install
                with ``pip install 'conduit-connector-sdk[avro]'``).
        """
        parsed = self._parsed
        if not isinstance(parsed, dict) or parsed.get("type") != "record":
            raise TypeError(
                "AvroSchema.encode: schema is not an Avro record -- AvroSchema is "
                "record-shaped (dict in, dict out), matching every OpenCDC payload use"
            )
        if not isinstance(value, dict):
            raise TypeError(
                f"AvroSchema.encode: expected a record (dict) matching the schema, "
                f"got {type(value).__name__}"
            )
        _validate_value(parsed, value, "", parsed.get("__named_schemas") or {})
        buf = io.BytesIO()
        _fastavro().schemaless_writer(buf, parsed, value)
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
        decoded = _fastavro().schemaless_reader(io.BytesIO(data), self._parsed)
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
