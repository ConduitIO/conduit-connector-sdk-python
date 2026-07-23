"""Tests for :mod:`conduit.config` -- ``BaseConfig``/``Field``/``to_parameters``.

Covers the design doc §2.2 mapping rules and its documented A-gaps
(``TYPE_DURATION``/``TYPE_EXCLUSION`` raise ``NotImplementedError`` rather
than guessing).
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
    interval: datetime.timedelta = Field(default=datetime.timedelta(seconds=5))


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


def test_duration_field_raises_not_implemented() -> None:
    """§2.2's A-gap: TYPE_DURATION has no pydantic-native mapping -- raise, don't guess."""
    with pytest.raises(NotImplementedError, match=r"TYPE_DURATION|duration"):
        to_parameters(_WithDuration)


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
