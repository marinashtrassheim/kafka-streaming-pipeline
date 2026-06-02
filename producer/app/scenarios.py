"""
Reproducible scenario plan for the 2-minute demo run.

Each event fires at `at_second` (0-based) from producer start.
`timestamp_offset_seconds` is applied relative to UTC send time.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ScenarioEvent:
    at_second: int
    scenario: str
    event_id: str
    user_id: int
    event_type: str
    product_id: Optional[int] = None
    product_name: str = ""
    quantity: int = 1
    price: float = 0.0
    timestamp_offset_seconds: int = -10
    session_id: str = ""


def _session(user_id: int) -> str:
    return f"sess_scenario_{user_id}"


# Laptop 101, Mouse 102, Keyboard 103
SCENARIO_EVENTS: list[ScenarioEvent] = [
    # --- Cart totals (user 1001) ---
    ScenarioEvent(
        0, "cart_add_laptop", "scn-1001-add-laptop", 1001, "add_to_cart",
        101, "Laptop", 1, 999.99, session_id=_session(1001),
    ),
    ScenarioEvent(
        4, "cart_add_mouse", "scn-1001-add-mouse", 1001, "add_to_cart",
        102, "Mouse", 1, 29.99, session_id=_session(1001),
    ),
    # --- Deduplication ---
    ScenarioEvent(
        8, "dedup_first", "scn-dedup-001", 1001, "add_to_cart",
        103, "Keyboard", 1, 79.99, session_id=_session(1001),
    ),
    ScenarioEvent(
        9, "dedup_repeat", "scn-dedup-001", 1001, "add_to_cart",
        103, "Keyboard", 1, 79.99, session_id=_session(1001),
    ),
    # --- update_quantity (user 1002) ---
    ScenarioEvent(
        12, "update_qty_setup", "scn-1002-add-mouse", 1002, "add_to_cart",
        102, "Mouse", 1, 29.99, session_id=_session(1002),
    ),
    ScenarioEvent(
        16, "update_quantity", "scn-1002-update-qty", 1002, "update_quantity",
        102, "Mouse", 3, 29.99, session_id=_session(1002),
    ),
    # --- Watermark advance (user 1001) ---
    ScenarioEvent(
        20, "watermark_advance", "scn-1001-add-case", 1001, "add_to_cart",
        103, "Keyboard", 1, 79.99, session_id=_session(1001),
    ),
    # --- Late within tolerance (~3 min late, limit 300s) ---
    ScenarioEvent(
        24, "late_within_tolerance", "scn-1001-late-ok", 1001, "add_to_cart",
        102, "Mouse", 1, 29.99, timestamp_offset_seconds=-180,
        session_id=_session(1001),
    ),
    # --- Late beyond tolerance (~10 min late) ---
    ScenarioEvent(
        28, "late_rejected", "scn-1001-late-reject", 1001, "add_to_cart",
        101, "Laptop", 1, 999.99, timestamp_offset_seconds=-600,
        session_id=_session(1001),
    ),
    # --- Minute aggregation burst (users 1003–1005) ---
    ScenarioEvent(
        45, "agg_user3", "scn-1003-add", 1003, "add_to_cart",
        101, "Laptop", 1, 999.99, session_id=_session(1003),
    ),
    ScenarioEvent(
        45, "agg_user4", "scn-1004-add", 1004, "add_to_cart",
        102, "Mouse", 1, 29.99, session_id=_session(1004),
    ),
    ScenarioEvent(
        46, "agg_user5", "scn-1005-add", 1005, "add_to_cart",
        103, "Keyboard", 1, 79.99, session_id=_session(1005),
    ),
    ScenarioEvent(
        55, "agg_user3_second", "scn-1003-add-2", 1003, "add_to_cart",
        102, "Mouse", 1, 29.99, session_id=_session(1003),
    ),
    # --- Checkout path (user 1002) ---
    ScenarioEvent(
        70, "remove_item", "scn-1002-remove", 1002, "remove_item",
        102, "Mouse", 3, 29.99, session_id=_session(1002),
    ),
    ScenarioEvent(
        72, "checkout", "scn-1002-checkout", 1002, "checkout",
        0, "", 0, 0.0, session_id=_session(1002),
    ),
    # --- Same-minute burst (user 1006) ---
    ScenarioEvent(
        90, "burst_a", "scn-1006-a", 1006, "add_to_cart",
        101, "Laptop", 1, 999.99, session_id=_session(1006),
    ),
    ScenarioEvent(
        90, "burst_b", "scn-1006-b", 1006, "add_to_cart",
        102, "Mouse", 1, 29.99, session_id=_session(1006),
    ),
    ScenarioEvent(
        91, "burst_c", "scn-1006-c", 1006, "add_to_cart",
        103, "Keyboard", 1, 79.99, session_id=_session(1006),
    ),
]


def events_by_second() -> dict[int, list[ScenarioEvent]]:
    """Group scenario events by the second they should be published."""
    grouped: dict[int, list[ScenarioEvent]] = {}
    for spec in SCENARIO_EVENTS:
        grouped.setdefault(spec.at_second, []).append(spec)
    return grouped
