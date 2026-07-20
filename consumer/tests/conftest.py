"""Shared fixtures for consumer unit tests."""
import sys
from datetime import datetime
from pathlib import Path

import pytest

CONSUMER_ROOT = Path(__file__).resolve().parent.parent
if str(CONSUMER_ROOT) not in sys.path:
    sys.path.insert(0, str(CONSUMER_ROOT))

from app.schemas import CartEvent  # noqa: E402


@pytest.fixture
def state_table():
    """Faust Tables support .get()/__setitem__/__delitem__ — a plain dict
    satisfies the same interface for unit tests."""
    return {}


@pytest.fixture
def make_event():
    """Factory for building valid CartEvent instances with sane defaults."""

    def _make(
        event_id: str = "evt-1",
        user_id: int = 1001,
        event_type: str = "add_to_cart",
        product_id: int = 101,
        product_name: str = "Widget",
        quantity: int = 1,
        price: float = 9.99,
        timestamp: datetime = None,
        session_id: str = "sess-1",
    ) -> CartEvent:
        return CartEvent(
            event_id=event_id,
            user_id=user_id,
            event_type=event_type,
            product_id=product_id,
            product_name=product_name,
            quantity=quantity,
            price=price,
            timestamp=timestamp or datetime(2026, 1, 1, 12, 0, 0),
            session_id=session_id,
        )

    return _make
