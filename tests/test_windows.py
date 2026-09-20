"""Tests for window parsing and feature-definition validation."""

import pytest

from streaming_feature_store.definitions import (
    FeatureDefinition,
    definition_from_dict,
    parse_window,
)


@pytest.mark.parametrize(
    "spec,expected",
    [
        ("30s", 30),
        ("1m", 60),
        ("5m", 300),
        ("1h", 3600),
        ("2h", 7200),
        ("1d", 86400),
        ("1H", 3600),  # case-insensitive
        ("  5m  ", 300),  # surrounding whitespace tolerated
    ],
)
def test_parse_window_valid(spec, expected):
    assert parse_window(spec) == expected


@pytest.mark.parametrize("spec", ["", "abc", "1x", "m", "0m", "-5m", "1.5h", None, 60])
def test_parse_window_invalid(spec):
    with pytest.raises(ValueError):
        parse_window(spec)


def test_definition_from_dict_ok():
    d = definition_from_dict(
        {
            "name": "purchase_sum_1h",
            "agg": "sum",
            "window": "1h",
            "field": "amount",
            "filters": {"event_type": "purchase"},
            "description": "hourly revenue",
        }
    )
    assert d.window_seconds == 3600
    assert d.to_dict()["field"] == "amount"


def test_definition_missing_key():
    with pytest.raises(ValueError):
        definition_from_dict({"name": "x", "agg": "count"})


def test_definition_bad_name():
    with pytest.raises(ValueError):
        FeatureDefinition(name="not a name!", agg="count", window="1h")


def test_definition_bad_agg():
    with pytest.raises(ValueError):
        FeatureDefinition(name="x", agg="median", window="1h")


def test_definition_sum_requires_field():
    with pytest.raises(ValueError):
        FeatureDefinition(name="x", agg="sum", window="1h")


def test_definition_count_needs_no_field():
    d = FeatureDefinition(name="clicks_5m", agg="count", window="5m")
    assert d.field is None
