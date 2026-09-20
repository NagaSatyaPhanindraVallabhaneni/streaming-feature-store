"""Feature definitions: window parsing and validation.

A feature definition describes one streaming aggregation, e.g.
"sum of purchase amounts per user over a 1-hour tumbling window".
Definitions are registered as JSON/YAML-style dicts via the API.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from dataclasses import field as dc_field

WINDOW_RE = re.compile(r"^\s*(\d+)\s*([smhd])\s*$", re.IGNORECASE)
_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
AGGS = ("count", "sum", "avg")
NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def parse_window(spec: str) -> int:
    """Parse a window spec like '30s', '5m', '1h', '1d' into seconds."""
    if not isinstance(spec, str):
        raise ValueError(f"Window must be a string like '5m', got {spec!r}")
    match = WINDOW_RE.match(spec)
    if not match:
        raise ValueError(
            f"Invalid window {spec!r}: expected <number><s|m|h|d>, e.g. '30s', '5m', '1h'"
        )
    amount = int(match.group(1))
    if amount <= 0:
        raise ValueError(f"Invalid window {spec!r}: amount must be positive")
    return amount * _UNIT_SECONDS[match.group(2).lower()]


@dataclass
class FeatureDefinition:
    """One streaming feature: aggregation over a tumbling time window."""

    name: str
    agg: str
    window: str
    field: str | None = None
    filters: dict = dc_field(default_factory=dict)
    description: str = ""
    window_seconds: int = dc_field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not NAME_RE.match(self.name):
            raise ValueError(
                f"Invalid feature name {self.name!r}: use letters, digits and underscores, "
                "starting with a letter or underscore"
            )
        if self.agg not in AGGS:
            raise ValueError(f"Invalid agg {self.agg!r}: expected one of {AGGS}")
        self.window_seconds = parse_window(self.window)
        if self.agg in ("sum", "avg") and not self.field:
            raise ValueError(f"agg={self.agg!r} requires a 'field' to aggregate")
        if not isinstance(self.filters, dict):
            raise ValueError(f"filters must be a dict of equality matches, got {self.filters!r}")

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "agg": self.agg,
            "window": self.window,
            "window_seconds": self.window_seconds,
            "field": self.field,
            "filters": dict(self.filters),
            "description": self.description,
        }


def definition_from_dict(data: dict) -> FeatureDefinition:
    """Build a validated FeatureDefinition from a plain dict (e.g. parsed JSON)."""
    if not isinstance(data, dict):
        raise ValueError(f"Definition must be a JSON object, got {type(data).__name__}")
    for key in ("name", "agg", "window"):
        if key not in data:
            raise ValueError(f"Definition is missing required key {key!r}")
    return FeatureDefinition(
        name=data["name"],
        agg=data["agg"],
        window=data["window"],
        field=data.get("field"),
        filters=dict(data.get("filters") or {}),
        description=data.get("description", ""),
    )
