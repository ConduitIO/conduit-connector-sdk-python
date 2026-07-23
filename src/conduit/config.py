"""Connector configuration: ``BaseConfig``, ``Field``, ``to_parameters()``.

See ``docs/design/20260707-python-connector-sdk.md`` §2.2. Go needs
``paramgen`` (a code-generation pass driven by struct tags like
``validate:"required,gt=0,lt=100,inclusion=a|b"``) because Go's runtime
reflection isn't rich enough to turn a struct's field types/tags into a
``config.Parameter`` map without a separate generation step. Pydantic v2's
``model_fields`` already carries type, default, and constraint metadata at
runtime, so this module introspects a model directly -- no codegen, no
``//go:generate``, always in sync with the model because it *is* the model.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import Any, Literal, get_args, get_origin

import annotated_types
import pydantic
from pydantic.fields import FieldInfo

import conduit._grpc  # noqa: F401  -- sets up sys.path, see conduit._grpc.__init__
from config.v1 import parameter_pb2 as _parameter_pb2

Field = pydantic.Field
"""Re-export of :func:`pydantic.Field`.

A thin re-export rather than a wrapper: pydantic v2's ``Field`` already
carries everything :func:`to_parameters` needs (``description``, ``ge``/
``le``/``gt``/``lt``, ``pattern``, ``default``) directly in
``model_fields``' ``FieldInfo``. Wrapping it would only add indirection for
no behavioral gain (no speculative generality, per ``CLAUDE.md``).
"""

# A boundary nudge used only to approximate an *inclusive* pydantic
# constraint (`ge=`/`le=`) as the wire protocol's *exclusive*
# `TYPE_GREATER_THAN`/`TYPE_LESS_THAN` validations for float-typed fields.
# See `_approximate_ge_as_gt`/`_approximate_le_as_lt` below -- this is a
# documented approximation, not exact, and only affects values within this
# epsilon of the declared boundary.
_FLOAT_BOUNDARY_EPSILON = 1e-9


class BaseConfig(pydantic.BaseModel):
    """Base class for connector configuration models.

    Subclass this with plain pydantic v2 field declarations (using
    :data:`Field` for descriptions/constraints); the SDK introspects the
    result via :func:`to_parameters` to build the ``Specify`` RPC's parameter
    map and uses the model itself (via ``model_validate``) to parse and
    validate the ``Configure`` RPC's ``config: map<string, string>`` payload.

    Example:
        >>> from typing import Literal
        >>> class Config(BaseConfig):
        ...     url: str = Field(description="HTTP endpoint to poll.")
        ...     poll_interval_ms: int = Field(
        ...         default=1000, ge=100, description="Delay between empty polls."
        ...     )
        ...     format: Literal["json", "csv"] = Field(default="json")
    """

    @classmethod
    def to_parameters(cls) -> dict[str, _parameter_pb2.Parameter]:
        """Introspect this model into the ``Specify`` RPC's parameter map.

        Convenience classmethod mirroring the design doc §2.2 ergonomic
        (``SourceConfig.to_parameters()``); delegates to the module-level
        :func:`to_parameters` function, which is the canonical
        implementation and also usable standalone.
        """
        return to_parameters(cls)


@dataclass(slots=True)
class Specification:
    """The static description of a connector plugin, returned by ``Specify``.

    Attributes:
        name: short, unique plugin name (e.g. ``"http-poll"``).
        version: semver-ish version string (e.g. ``"0.1.0"``).
        author: author name or organization.
        summary: one-line summary.
        description: longer, multi-line description.

    ``source_params``/``destination_params`` are deliberately *not* fields
    here: they're computed by :mod:`conduit.serve` from whichever
    ``Source``/``Destination`` subclass's ``Config`` was registered, via
    :func:`to_parameters`, at ``Specify`` RPC handling time -- keeping this
    dataclass a plain, author-supplied literal (matching the design doc
    §2.7 call shape: ``Specification(name=..., version=..., author=...)``).
    """

    name: str
    version: str
    author: str
    summary: str = ""
    description: str = ""


def to_parameters(config_cls: type[BaseConfig]) -> dict[str, _parameter_pb2.Parameter]:
    """Introspect a :class:`BaseConfig` subclass into ``config.Parameter``s.

    Mapping rules (design doc §2.2):

    - A field with no default -> ``Validation.TYPE_REQUIRED``.
    - ``gt=``/``lt=`` -> exact ``TYPE_GREATER_THAN``/``TYPE_LESS_THAN``
      (the wire validation types are themselves exclusive, matching
      pydantic's ``gt``/``lt`` exactly).
    - ``ge=``/``le=`` -> **approximated** as ``TYPE_GREATER_THAN``/
      ``TYPE_LESS_THAN`` by nudging the boundary so the declared value
      itself still validates: for ``int``-typed fields this is exact
      (boundary ``- 1``/``+ 1``); for ``float``-typed fields this uses a
      small (``1e-9``) epsilon nudge, which is an approximation, not exact
      -- see :data:`_FLOAT_BOUNDARY_EPSILON`. Documented here rather than
      silently producing a subtly-wrong validation.
    - ``Literal[...]`` -> one ``Validation.TYPE_INCLUSION`` entry per
      literal value (``Parameter.validations`` is ``repeated Validation``,
      matching the Go SDK's shape).
    - ``pattern=`` -> ``Validation.TYPE_REGEX``.

    **Explicitly not attempted (open A-gaps, design doc §2.2, non-blocking
    for Phase 1):**

    - ``Parameter.Type.TYPE_DURATION`` (Go's ``"5s"``-style duration
      strings, not ISO-8601) has no pydantic-native mapping. A field typed
      ``datetime.timedelta`` raises ``NotImplementedError`` rather than
      guessing at a wrong mapping.
    - ``Validation.Type.TYPE_EXCLUSION`` has no pydantic-native constraint
      to introspect. A field requesting it via
      ``Field(json_schema_extra={"exclusion": [...]})`` raises
      ``NotImplementedError`` rather than silently dropping the
      constraint.

    Args:
        config_cls: a :class:`BaseConfig` subclass (not instance).

    Returns:
        A mapping of field name -> wire ``config.Parameter``, suitable for
        ``Specifier.Specify.Response.source_params``/``destination_params``.

    Raises:
        NotImplementedError: if a field requests ``TYPE_DURATION`` or
            ``TYPE_EXCLUSION`` semantics (see above).
    """
    return {name: _field_to_parameter(name, info) for name, info in config_cls.model_fields.items()}


def _field_to_parameter(name: str, info: FieldInfo) -> _parameter_pb2.Parameter:
    if info.annotation in (datetime.timedelta,):
        raise NotImplementedError(
            f"config field {name!r}: `datetime.timedelta` (duration) has no "
            "pydantic-native mapping to config.Parameter.TYPE_DURATION yet -- "
            "open A-gap, docs/design/20260707-python-connector-sdk.md §2.2. "
            "Use a plain int (milliseconds) or str field with duration "
            "semantics documented in the field description instead."
        )
    extra = info.json_schema_extra if isinstance(info.json_schema_extra, dict) else {}
    if "exclusion" in extra:
        raise NotImplementedError(
            f"config field {name!r}: Validation.TYPE_EXCLUSION has no "
            "pydantic-native mapping yet -- open A-gap, "
            "docs/design/20260707-python-connector-sdk.md §2.2."
        )

    validations: list[_parameter_pb2.Validation] = []
    if info.is_required():
        validations.append(_parameter_pb2.Validation(type=_parameter_pb2.Validation.TYPE_REQUIRED))

    param_type, literal_values = _resolve_type(info.annotation)
    for value in literal_values:
        validations.append(
            _parameter_pb2.Validation(
                type=_parameter_pb2.Validation.TYPE_INCLUSION, value=str(value)
            )
        )

    is_int = param_type == _parameter_pb2.Parameter.TYPE_INT
    validations.extend(_constraint_validations(info, is_int=is_int))

    if info.is_required():
        default = ""
    else:
        default = _format_default(info.get_default(call_default_factory=True))

    return _parameter_pb2.Parameter(
        default=default,
        description=info.description or "",
        type=param_type,
        validations=validations,
    )


def _unwrap_optional(annotation: Any) -> Any:
    """Unwrap a single-level ``X | None`` to ``X``; pass through otherwise."""
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin is not None and type(None) in args:
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return non_none[0]
    return annotation


_ParamTypeAndLiterals = tuple["_parameter_pb2.Parameter.Type", tuple[Any, ...]]


def _resolve_type(annotation: Any) -> _ParamTypeAndLiterals:
    """Resolve a field annotation to a wire ``Parameter.Type`` + literal values.

    Returns:
        A ``(param_type, literal_values)`` pair. ``literal_values`` is
        non-empty only for ``Literal[...]`` annotations, and drives the
        ``TYPE_INCLUSION`` validations built by the caller.
    """
    annotation = _unwrap_optional(annotation)

    if get_origin(annotation) is Literal:
        literal_values = get_args(annotation)
        if all(isinstance(v, bool) for v in literal_values):
            return _parameter_pb2.Parameter.TYPE_BOOL, literal_values
        if all(isinstance(v, int) for v in literal_values):
            return _parameter_pb2.Parameter.TYPE_INT, literal_values
        # Mixed or non-int/bool literal members (str is the common case) --
        # fall back to TYPE_STRING, documented rather than guessed silently.
        return _parameter_pb2.Parameter.TYPE_STRING, literal_values

    if annotation is bool:
        return _parameter_pb2.Parameter.TYPE_BOOL, ()
    if annotation is int:
        return _parameter_pb2.Parameter.TYPE_INT, ()
    if annotation is float:
        return _parameter_pb2.Parameter.TYPE_FLOAT, ()
    if annotation is str:
        return _parameter_pb2.Parameter.TYPE_STRING, ()

    # Unknown/unsupported annotation (e.g. a nested BaseModel, a custom
    # type): fall back to TYPE_STRING. This is a deliberate, documented
    # approximation -- not a silent guess dressed up as a mapping -- for
    # anything this Phase-1 introspection doesn't have a precise wire type
    # for.
    return _parameter_pb2.Parameter.TYPE_STRING, ()


def _constraint_validations(info: FieldInfo, *, is_int: bool) -> list[_parameter_pb2.Validation]:
    validations: list[_parameter_pb2.Validation] = []
    for constraint in info.metadata:
        if isinstance(constraint, annotated_types.Gt):
            validations.append(
                _parameter_pb2.Validation(
                    type=_parameter_pb2.Validation.TYPE_GREATER_THAN,
                    value=str(constraint.gt),
                )
            )
        elif isinstance(constraint, annotated_types.Lt):
            validations.append(
                _parameter_pb2.Validation(
                    type=_parameter_pb2.Validation.TYPE_LESS_THAN,
                    value=str(constraint.lt),
                )
            )
        elif isinstance(constraint, annotated_types.Ge):
            validations.append(
                _parameter_pb2.Validation(
                    type=_parameter_pb2.Validation.TYPE_GREATER_THAN,
                    value=_approximate_ge_as_gt(constraint.ge, is_int=is_int),
                )
            )
        elif isinstance(constraint, annotated_types.Le):
            validations.append(
                _parameter_pb2.Validation(
                    type=_parameter_pb2.Validation.TYPE_LESS_THAN,
                    value=_approximate_le_as_lt(constraint.le, is_int=is_int),
                )
            )
        else:
            pattern = getattr(constraint, "pattern", None)
            if pattern is not None:
                validations.append(
                    _parameter_pb2.Validation(
                        type=_parameter_pb2.Validation.TYPE_REGEX, value=str(pattern)
                    )
                )
    return validations


def _approximate_ge_as_gt(ge: Any, *, is_int: bool) -> str:
    """Approximate an inclusive ``ge=`` bound as the wire's exclusive ``gt``.

    ``ge`` is typed ``Any`` because ``annotated_types.Ge.ge`` is itself
    typed against a ``SupportsGe`` structural protocol, not a concrete
    numeric type -- in practice pydantic only ever populates it from
    ``Field(ge=...)``, which authors pass an ``int``/``float``.

    Exact for ``int``-typed fields (``ge - 1`` admits exactly the same
    integers as ``ge`` would inclusively). For ``float``-typed fields this
    nudges the boundary down by :data:`_FLOAT_BOUNDARY_EPSILON`, which is an
    approximation: values within that epsilon of ``ge`` are handled
    correctly, but this is not bit-exact inclusive-boundary semantics.
    """
    if is_int:
        return str(int(ge) - 1)
    return repr(float(ge) - _FLOAT_BOUNDARY_EPSILON)


def _approximate_le_as_lt(le: Any, *, is_int: bool) -> str:
    """Approximate an inclusive ``le=`` bound as the wire's exclusive ``lt``.

    See :func:`_approximate_ge_as_gt` for why ``le`` is typed ``Any`` --
    exact for ``int``, epsilon-nudged approximation for ``float``.
    """
    if is_int:
        return str(int(le) + 1)
    return repr(float(le) + _FLOAT_BOUNDARY_EPSILON)


def _format_default(value: Any) -> str:
    """Render a Python default value as the wire's ``Parameter.default`` string.

    Booleans use lowercase ``"true"``/``"false"`` (matching Go's
    ``strconv.FormatBool``/JSON convention); ``None`` becomes an empty
    string (an accepted approximation -- the wire has no way to distinguish
    "no default" from "default is explicitly None", but a required field
    with no default already gets ``""`` too via :func:`_field_to_parameter`,
    so this is consistent within this SDK's own mapping even if not
    perfectly round-trippable against a hypothetical Go-authored consumer
    inspecting ``Parameter.default`` for ``None``-ness specifically).
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


__all__ = [
    "BaseConfig",
    "Field",
    "Specification",
    "to_parameters",
]
