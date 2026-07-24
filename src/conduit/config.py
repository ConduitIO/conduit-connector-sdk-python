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
import re
from collections.abc import Mapping
from dataclasses import dataclass
from fractions import Fraction
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

    @pydantic.model_validator(mode="before")
    @classmethod
    def _parse_go_durations_before_validation(cls, data: Any) -> Any:
        """Parse Go-duration-syntax strings for ``timedelta``-typed fields.

        The ``Configure`` RPC's config map is always ``map<string, string>``
        on the wire (see the wire contract facts) -- pydantic v2 has no
        built-in support for Go's ``"5s"``/``"1h30m"`` duration syntax (it
        understands ISO-8601 durations and bare numeric seconds, not this).
        This ``model_validator(mode="before")`` runs ahead of pydantic's own
        field validation and converts any string value destined for a
        ``datetime.timedelta``-typed field into an actual ``timedelta`` via
        :func:`parse_go_duration`, so the rest of validation proceeds
        exactly as if a real ``timedelta`` had been passed in. Symmetric
        with :func:`format_go_duration`, used by :func:`to_parameters` to
        serialize a ``timedelta`` default for the ``Specify`` RPC -- see
        that function for the exact wire format both sides agree on.

        Non-string values (e.g. an author constructing the model directly
        with a real ``timedelta``, as in tests) pass through unchanged.
        """
        if not isinstance(data, Mapping):
            return data
        converted = dict(data)
        for name, info in cls.model_fields.items():
            if _unwrap_optional(info.annotation) is not datetime.timedelta:
                continue
            value = converted.get(name)
            if isinstance(value, str):
                converted[name] = parse_go_duration(value)
        return converted


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
      itself still validates: for ``int``-typed (and ``timedelta``-typed --
      both are fundamentally discrete, integer-microsecond-resolution
      types) fields this is exact (boundary ``- 1``/``+ 1`` unit); for
      ``float``-typed fields this uses a small (``1e-9``) epsilon nudge,
      which is an approximation, not exact -- see
      :data:`_FLOAT_BOUNDARY_EPSILON`. Documented here rather than
      silently producing a subtly-wrong validation.
    - ``Literal[...]`` -> one ``Validation.TYPE_INCLUSION`` entry per
      literal value (``Parameter.validations`` is ``repeated Validation``,
      matching the Go SDK's shape).
    - ``pattern=`` -> ``Validation.TYPE_REGEX``.
    - ``datetime.timedelta`` -> ``Parameter.Type.TYPE_DURATION``, with the
      default (if any) serialized via :func:`format_go_duration` into Go's
      ``time.Duration.String()`` syntax (``"5s"``, ``"1h30m"``, ``"500ms"``,
      not ISO-8601). :class:`BaseConfig`'s ``model_validator`` parses that
      same syntax back (:func:`parse_go_duration`) when the ``Configure``
      RPC's string config map arrives -- see that validator's docstring.
      This closes what was previously an open A-gap (a plain
      ``NotImplementedError``); see git history for the prior wording if
      you're looking for why this changed.

    **Still an open A-gap, non-blocking for Phase 1:**

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
        NotImplementedError: if a field requests ``TYPE_EXCLUSION``
            semantics (see above).
    """
    return {name: _field_to_parameter(name, info) for name, info in config_cls.model_fields.items()}


def _field_to_parameter(name: str, info: FieldInfo) -> _parameter_pb2.Parameter:
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
    is_duration = param_type == _parameter_pb2.Parameter.TYPE_DURATION
    validations.extend(_constraint_validations(info, is_int=is_int, is_duration=is_duration))

    if info.is_required():
        default = ""
    elif is_duration:
        default_value = info.get_default(call_default_factory=True)
        default = "" if default_value is None else format_go_duration(default_value)
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
    if annotation is datetime.timedelta:
        return _parameter_pb2.Parameter.TYPE_DURATION, ()

    # Unknown/unsupported annotation (e.g. a nested BaseModel, a custom
    # type): fall back to TYPE_STRING. This is a deliberate, documented
    # approximation -- not a silent guess dressed up as a mapping -- for
    # anything this Phase-1 introspection doesn't have a precise wire type
    # for.
    return _parameter_pb2.Parameter.TYPE_STRING, ()


def _constraint_validations(
    info: FieldInfo, *, is_int: bool, is_duration: bool = False
) -> list[_parameter_pb2.Validation]:
    validations: list[_parameter_pb2.Validation] = []
    for constraint in info.metadata:
        if isinstance(constraint, annotated_types.Gt):
            validations.append(
                _parameter_pb2.Validation(
                    type=_parameter_pb2.Validation.TYPE_GREATER_THAN,
                    value=_format_bound(constraint.gt, is_duration=is_duration),
                )
            )
        elif isinstance(constraint, annotated_types.Lt):
            validations.append(
                _parameter_pb2.Validation(
                    type=_parameter_pb2.Validation.TYPE_LESS_THAN,
                    value=_format_bound(constraint.lt, is_duration=is_duration),
                )
            )
        elif isinstance(constraint, annotated_types.Ge):
            ge_value = _approximate_ge_as_gt(constraint.ge, is_int=is_int, is_duration=is_duration)
            validations.append(
                _parameter_pb2.Validation(
                    type=_parameter_pb2.Validation.TYPE_GREATER_THAN,
                    value=ge_value,
                )
            )
        elif isinstance(constraint, annotated_types.Le):
            le_value = _approximate_le_as_lt(constraint.le, is_int=is_int, is_duration=is_duration)
            validations.append(
                _parameter_pb2.Validation(
                    type=_parameter_pb2.Validation.TYPE_LESS_THAN,
                    value=le_value,
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


def _format_bound(value: Any, *, is_duration: bool) -> str:
    """Format an exact (``gt=``/``lt=``) bound value for the wire.

    ``timedelta`` bounds use :func:`format_go_duration` (Go duration
    syntax); everything else uses plain ``str()``.
    """
    if is_duration:
        return format_go_duration(value)
    return str(value)


def _approximate_ge_as_gt(ge: Any, *, is_int: bool, is_duration: bool = False) -> str:
    """Approximate an inclusive ``ge=`` bound as the wire's exclusive ``gt``.

    ``ge`` is typed ``Any`` because ``annotated_types.Ge.ge`` is itself
    typed against a ``SupportsGe`` structural protocol, not a concrete
    numeric type -- in practice pydantic only ever populates it from
    ``Field(ge=...)``, which authors pass an ``int``/``float``/``timedelta``.

    Exact for ``int``-typed **and** ``timedelta``-typed fields (both are
    fundamentally discrete types at the resolution that matters here --
    whole integers, or whole microseconds -- so ``ge - 1 unit`` admits
    exactly the same values ``ge`` would inclusively). For ``float``-typed
    fields this nudges the boundary down by :data:`_FLOAT_BOUNDARY_EPSILON`,
    which is an approximation: values within that epsilon of ``ge`` are
    handled correctly, but this is not bit-exact inclusive-boundary
    semantics.
    """
    if is_duration:
        return format_go_duration(ge - datetime.timedelta(microseconds=1))
    if is_int:
        return str(int(ge) - 1)
    return repr(float(ge) - _FLOAT_BOUNDARY_EPSILON)


def _approximate_le_as_lt(le: Any, *, is_int: bool, is_duration: bool = False) -> str:
    """Approximate an inclusive ``le=`` bound as the wire's exclusive ``lt``.

    See :func:`_approximate_ge_as_gt` for why ``le`` is typed ``Any`` --
    exact for ``int``/``timedelta``, epsilon-nudged approximation for
    ``float``.
    """
    if is_duration:
        return format_go_duration(le + datetime.timedelta(microseconds=1))
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


# Go duration units, ordered by microsecond magnitude, smallest first --
# used only by the parser below (the formatter picks units explicitly).
# `Fraction` keeps arithmetic exact (no float rounding) while accumulating
# a parsed duration string's components before the final round-to-nearest-
# microsecond conversion into a `datetime.timedelta` (which itself has no
# finer resolution than microseconds -- matching Go's `ns` precision
# exactly isn't possible in a stdlib `timedelta`, and isn't needed for
# connector config durations).
_GO_DURATION_UNIT_TO_MICROSECONDS: dict[str, Fraction] = {
    "ns": Fraction(1, 1000),
    "us": Fraction(1),
    "µs": Fraction(1),  # µs, Go's own preferred spelling
    "ms": Fraction(1000),
    "s": Fraction(1_000_000),
    "m": Fraction(60_000_000),
    "h": Fraction(3_600_000_000),
}
_GO_DURATION_COMPONENT_RE = re.compile(r"([0-9]*\.?[0-9]+)(ns|µs|us|ms|s|m|h)")


def format_go_duration(value: datetime.timedelta) -> str:
    """Render a ``timedelta`` as a Go ``time.Duration.String()``-syntax string.

    Matches Go's own formatting rules for the common cases this SDK's
    config fields need (design doc §2.2's Duration A-gap, now closed):
    microseconds/milliseconds/seconds for sub-minute durations, and
    ``[Xh][Ym]Zs`` for durations of a minute or more (hours omitted if
    zero; minutes shown whenever hours are, or whenever there are any,
    matching e.g. ``time.Duration(time.Hour).String() == "1h0m0s"`` and
    ``(90 * time.Second).String() == "1m30s"``). Round-trips exactly
    through :func:`parse_go_duration` for any value a ``timedelta`` can
    represent (microsecond resolution).

    Args:
        value: the duration to format.

    Returns:
        A Go-duration-syntax string, e.g. ``"5s"``, ``"1h30m"``... except
        this function always includes a trailing seconds component for
        the ``>= 1 minute`` branch (``"1h30m0s"``, not ``"1h30m"``) --
        matching Go's own ``String()`` output exactly, which always prints
        seconds.
    """
    total_us = value.days * 86_400_000_000 + value.seconds * 1_000_000 + value.microseconds
    if total_us == 0:
        return "0s"

    sign = "-" if total_us < 0 else ""
    total_us = abs(total_us)

    if total_us < 1_000:
        return f"{sign}{total_us}µs"
    if total_us < 1_000_000:
        return f"{sign}{_format_scaled(total_us, 1_000)}ms"
    if total_us < 60_000_000:
        return f"{sign}{_format_scaled(total_us, 1_000_000)}s"

    total_seconds, sub_second_us = divmod(total_us, 1_000_000)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    seconds_us = seconds * 1_000_000 + sub_second_us

    parts: list[str] = []
    if hours:
        parts.append(f"{hours}h")
    if hours or minutes:
        parts.append(f"{minutes}m")
    parts.append(f"{_format_scaled(seconds_us, 1_000_000)}s")
    return sign + "".join(parts)


def _format_scaled(total: int, unit: int) -> str:
    """Format ``total`` (an integer count of the base unit) scaled by ``unit``.

    E.g. ``_format_scaled(1500, 1000)`` (1500 microseconds, scaled to
    milliseconds) -> ``"1.5"``. Uses exact integer arithmetic throughout
    (no floats), so there's no rounding-representation mismatch between
    what's formatted and what :func:`parse_go_duration` reads back.
    """
    whole, rem = divmod(total, unit)
    if rem == 0:
        return str(whole)
    digits = len(str(unit)) - 1
    frac = str(rem).rjust(digits, "0").rstrip("0")
    return f"{whole}.{frac}"


def parse_go_duration(value: str) -> datetime.timedelta:
    """Parse a Go ``time.Duration``-syntax string into a ``timedelta``.

    Accepts a signed sequence of ``<number><unit>`` components (each
    number may have a fractional part), e.g. ``"5s"``, ``"1h30m"``,
    ``"500ms"``, ``"-1.5h"``, ``"2h45m30s"``, ``"90s"``. Units: ``ns``,
    ``us``/``µs``, ``ms``, ``s``, ``m``, ``h`` -- matching Go's
    ``ParseDuration``. A bare ``"0"`` (no unit) is accepted as zero,
    matching Go. Arithmetic is done with exact ``fractions.Fraction``
    values (not floats) until the final round to the nearest whole
    microsecond, the finest resolution ``datetime.timedelta`` supports.

    Args:
        value: a Go-duration-syntax string.

    Returns:
        The equivalent ``timedelta``.

    Raises:
        ValueError: if ``value`` isn't valid Go duration syntax.
    """
    original = value
    s = value.strip()
    if not s:
        raise ValueError(f"invalid duration {original!r}: empty string")

    sign = 1
    if s[0] in "+-":
        sign = -1 if s[0] == "-" else 1
        s = s[1:]

    if s == "0":
        return datetime.timedelta(0)

    total = Fraction(0)
    pos = 0
    matched_any = False
    for match in _GO_DURATION_COMPONENT_RE.finditer(s):
        if match.start() != pos:
            raise ValueError(
                f"invalid duration {original!r}: unexpected characters at "
                f"position {pos} (Go duration syntax, e.g. '5s', '1h30m', '500ms')"
            )
        number = Fraction(match.group(1))
        unit = match.group(2)
        total += number * _GO_DURATION_UNIT_TO_MICROSECONDS[unit]
        pos = match.end()
        matched_any = True

    if not matched_any or pos != len(s):
        raise ValueError(
            f"invalid duration {original!r}: does not match Go duration syntax "
            "(e.g. '5s', '1h30m', '500ms', 'us'/'µs', 'ns')"
        )

    return datetime.timedelta(microseconds=sign * round(total))


__all__ = [
    "BaseConfig",
    "Field",
    "Specification",
    "format_go_duration",
    "parse_go_duration",
    "to_parameters",
]
