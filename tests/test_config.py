"""Tests for :mod:`conduit.config` -- ``BaseConfig``/``Field``/``to_parameters``.

Covers the design doc §2.2 mapping rules. ``TYPE_DURATION`` (``timedelta``
fields) is now a real, round-tripping mapping -- see
``tests/test_go_duration.py`` for the ``format_go_duration``/
``parse_go_duration`` unit tests specifically. ``TYPE_EXCLUSION`` remains an
open A-gap and still raises ``NotImplementedError``.
"""

from __future__ import annotations

import datetime
from typing import Literal

import pytest

import conduit._grpc  # noqa: F401  -- sets up sys.path, see conduit._grpc.__init__
from conduit.config import BaseConfig, Field, Specification, to_parameters
from config.v1 import parameter_pb2


class _RequiredOnly(BaseConfig):
    url: str = Field(description="a required string")


class _WithBounds(BaseConfig):
    ge_int: int = Field(default=1000, ge=100)
    le_int: int = Field(default=1, le=100)
    gt_float: float = Field(default=1.0, gt=0.0)
    lt_float: float = Field(default=1.0, lt=100.0)


class _WithLiteral(BaseConfig):
    format: Literal["json", "csv"] = Field(default="json")


class _WithPattern(BaseConfig):
    name: str = Field(default="x", pattern=r"^[a-z]+$")


class _WithBool(BaseConfig):
    enabled: bool = Field(default=False)


class _WithDuration(BaseConfig):
    interval: datetime.timedelta = Field(
        default=datetime.timedelta(seconds=5), description="poll interval"
    )
    timeout: datetime.timedelta = Field(description="required timeout, no default")
    long_wait: datetime.timedelta = Field(default=datetime.timedelta(hours=1, minutes=30))


def test_required_field_gets_required_validation() -> None:
    params = to_parameters(_RequiredOnly)
    param = params["url"]
    assert param.type == parameter_pb2.Parameter.TYPE_STRING
    assert param.default == ""
    types = [v.type for v in param.validations]
    assert parameter_pb2.Validation.TYPE_REQUIRED in types


def test_field_with_default_is_not_required() -> None:
    params = to_parameters(_WithBounds)
    types = [v.type for v in params["ge_int"].validations]
    assert parameter_pb2.Validation.TYPE_REQUIRED not in types


def test_ge_approximated_as_greater_than_admitting_the_boundary() -> None:
    """``ge=100`` should admit ``100`` itself -- approximated as ``gt=99`` for ints."""
    params = to_parameters(_WithBounds)
    validations = params["ge_int"].validations
    assert len(validations) == 1
    assert validations[0].type == parameter_pb2.Validation.TYPE_GREATER_THAN
    assert validations[0].value == "99"


def test_le_approximated_as_less_than_admitting_the_boundary() -> None:
    """``le=100`` should admit ``100`` itself -- approximated as ``lt=101`` for ints."""
    params = to_parameters(_WithBounds)
    validations = params["le_int"].validations
    assert len(validations) == 1
    assert validations[0].type == parameter_pb2.Validation.TYPE_LESS_THAN
    assert validations[0].value == "101"


def test_gt_maps_exactly_no_approximation() -> None:
    params = to_parameters(_WithBounds)
    validations = params["gt_float"].validations
    assert len(validations) == 1
    assert validations[0].type == parameter_pb2.Validation.TYPE_GREATER_THAN
    assert validations[0].value == "0.0"


def test_lt_maps_exactly_no_approximation() -> None:
    params = to_parameters(_WithBounds)
    validations = params["lt_float"].validations
    assert len(validations) == 1
    assert validations[0].type == parameter_pb2.Validation.TYPE_LESS_THAN
    assert validations[0].value == "100.0"


def test_literal_produces_one_inclusion_validation_per_value() -> None:
    params = to_parameters(_WithLiteral)
    validations = params["format"].validations
    assert {v.value for v in validations} == {"json", "csv"}
    assert all(v.type == parameter_pb2.Validation.TYPE_INCLUSION for v in validations)
    assert params["format"].type == parameter_pb2.Parameter.TYPE_STRING


def test_pattern_maps_to_regex_validation() -> None:
    params = to_parameters(_WithPattern)
    validations = params["name"].validations
    assert len(validations) == 1
    assert validations[0].type == parameter_pb2.Validation.TYPE_REGEX
    assert validations[0].value == r"^[a-z]+$"


def test_bool_field_maps_to_type_bool_with_lowercase_default() -> None:
    params = to_parameters(_WithBool)
    param = params["enabled"]
    assert param.type == parameter_pb2.Parameter.TYPE_BOOL
    assert param.default == "false"


def test_duration_field_maps_to_type_duration_with_go_syntax_default() -> None:
    """A ``timedelta`` field maps to ``TYPE_DURATION``; the default is Go-duration syntax."""
    params = to_parameters(_WithDuration)
    interval = params["interval"]
    assert interval.type == parameter_pb2.Parameter.TYPE_DURATION
    assert interval.default == "5s"
    assert interval.description == "poll interval"

    long_wait = params["long_wait"]
    assert long_wait.default == "1h30m0s"


def test_duration_field_with_no_default_is_required_and_has_empty_default() -> None:
    params = to_parameters(_WithDuration)
    timeout = params["timeout"]
    assert timeout.type == parameter_pb2.Parameter.TYPE_DURATION
    assert timeout.default == ""
    types = [v.type for v in timeout.validations]
    assert parameter_pb2.Validation.TYPE_REQUIRED in types


def test_configure_parses_go_duration_string_config_value() -> None:
    """The ``Configure`` RPC's string config map parses Go-duration syntax for timedelta fields."""
    config = _WithDuration.model_validate(
        {"interval": "10s", "timeout": "1h30m", "long_wait": "2h"}
    )
    assert config.interval == datetime.timedelta(seconds=10)
    assert config.timeout == datetime.timedelta(hours=1, minutes=30)
    assert config.long_wait == datetime.timedelta(hours=2)


def test_direct_construction_with_a_real_timedelta_still_works() -> None:
    """Non-string (already-``timedelta``) values pass through the before-validator unchanged."""
    config = _WithDuration(
        interval=datetime.timedelta(seconds=1),
        timeout=datetime.timedelta(minutes=5),
    )
    assert config.interval == datetime.timedelta(seconds=1)
    assert config.timeout == datetime.timedelta(minutes=5)


def test_exclusion_request_raises_not_implemented() -> None:
    """§2.2's A-gap: TYPE_EXCLUSION has no pydantic-native mapping -- raise, don't guess."""

    class _WithExclusion(BaseConfig):
        value: str = Field(default="x", json_schema_extra={"exclusion": ["a", "b"]})

    with pytest.raises(NotImplementedError, match=r"TYPE_EXCLUSION|exclusion"):
        to_parameters(_WithExclusion)


def test_base_config_classmethod_matches_module_function() -> None:
    assert _RequiredOnly.to_parameters() == to_parameters(_RequiredOnly)


def test_specification_is_a_plain_literal_dataclass() -> None:
    spec = Specification(name="http-poll", version="0.1.0", author="you")
    assert spec.name == "http-poll"
    assert spec.summary == ""
    assert spec.description == ""
