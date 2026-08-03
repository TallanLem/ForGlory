"""Arithmetic helpers for cumulative game counters with 32-bit rollovers."""

from __future__ import annotations

from typing import SupportsInt

UINT32_MODULUS = 1 << 32
INT32_MIN = -(1 << 31)

# Only genuinely cumulative counters are unwrapped. Level, glory, friends and
# combat stats may legitimately decrease and must never receive a 2**32 offset.
WRAP_COUNTER_COLUMNS: tuple[str, ...] = (
    "wins",
    "losses",
    "dragon_wins",
    "snake_wins",
    "lord_wins",
    "beasts_killed",
    "rob_silver",
    "lost_silver",
    "rob_crystals",
    "lost_crystals",
    "expedition_time",
    "quests_completed",
    "crystals_mined",
    "gifts_sent",
    "fish_caught",
    "plants_grown",
    "goblins_killed",
    "dragons_killed",
    "snakes_killed",
    "chat",
)
WRAP_COUNTER_COLUMN_SET = frozenset(WRAP_COUNTER_COLUMNS)


def unwrap_cumulative_counter(
    raw_value: SupportsInt,
    previous_value: SupportsInt | None,
) -> int:
    """Return a continuous representation of one cumulative 32-bit counter.

    The source has emitted a mixture of full unsigned totals and values wrapped
    at 2**32. A rollover is identifiable as a backward jump larger than the
    signed 32-bit range. Only those large backward jumps receive an offset.
    Large positive values are deliberately preserved: folding them downward
    would corrupt a valid absolute total when no earlier rollover evidence is
    available.
    """
    value = int(raw_value)
    if previous_value is None:
        while value < 0:
            value += UINT32_MODULUS
        return value

    previous = int(previous_value)
    while value - previous < INT32_MIN:
        value += UINT32_MODULUS
    return value


def cumulative_delta32(
    current: SupportsInt | None,
    previous: SupportsInt | None,
) -> int | None:
    """Return a rollover-safe adjacent delta for a cumulative counter."""
    if current is None or previous is None:
        return None
    normalized = unwrap_cumulative_counter(current, previous)
    return normalized - int(previous)


def sql_cumulative_delta32(
    current_expression: str,
    previous_expression: str,
) -> str:
    """Build the SQLite equivalent of :func:`cumulative_delta32`."""
    current = f"({current_expression})"
    previous = f"({previous_expression})"
    raw = f"({current}-{previous})"
    return (
        "CASE "
        f"WHEN {current} IS NULL OR {previous} IS NULL THEN NULL "
        f"WHEN {raw}<{INT32_MIN} THEN {raw}+{UINT32_MODULUS} "
        f"ELSE {raw} END"
    )
