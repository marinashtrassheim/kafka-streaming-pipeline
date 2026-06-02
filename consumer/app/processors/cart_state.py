"""
Per-user cart state in Faust (operational state for enrichment).
Minute/hour analytics are in ClickHouse (see migrations/002).
"""

from datetime import datetime
from typing import Dict, Optional
import logging
from dataclasses import dataclass, field

from app.schemas import CartEvent, EnrichedCartEvent


@dataclass
class UserCartState:
    """Immutable representation of user's cart state"""
    user_id: int
    items: Dict[int, int] = field(default_factory=dict)  # product_id -> quantity
    total_value: float = 0.0
    last_updated: datetime = field(default_factory=datetime.now)
    event_count: int = 0

    def add_item(self, product_id: int, quantity: int, price: float) -> 'UserCartState':
        """Return new state with item added"""
        new_items = self.items.copy()
        new_items[product_id] = new_items.get(product_id, 0) + quantity
        return UserCartState(
            user_id=self.user_id,
            items=new_items,
            total_value=self.total_value + (price * quantity),
            last_updated=datetime.now(),
            event_count=self.event_count + 1
        )

    def remove_item(self, product_id: int, quantity: int, price: float) -> 'UserCartState':
        """Return new state with item removed"""
        if product_id not in self.items:
            return self

        new_items = self.items.copy()
        current_qty = new_items[product_id]
        removed_qty = min(current_qty, quantity)

        if removed_qty >= current_qty:
            del new_items[product_id]
        else:
            new_items[product_id] = current_qty - removed_qty

        return UserCartState(
            user_id=self.user_id,
            items=new_items,
            total_value=self.total_value - (price * removed_qty),
            last_updated=datetime.now(),
            event_count=self.event_count + 1
        )

    def update_quantity(
        self,
        product_id: int,
        quantity: int,
        price: float,
    ) -> 'UserCartState':
        """Set product quantity (remove line if quantity <= 0)."""
        if product_id not in self.items:
            return self

        new_items = self.items.copy()
        old_qty = new_items[product_id]

        if quantity <= 0:
            del new_items[product_id]
            value_delta = -(old_qty * price)
        else:
            new_items[product_id] = quantity
            value_delta = (quantity - old_qty) * price

        return UserCartState(
            user_id=self.user_id,
            items=new_items,
            total_value=max(0.0, self.total_value + value_delta),
            last_updated=datetime.now(),
            event_count=self.event_count + 1,
        )

    def checkout(self) -> 'UserCartState':
        """Return new state after checkout (cart cleared)"""
        return UserCartState(
            user_id=self.user_id,
            items={},
            total_value=0.0,
            last_updated=datetime.now(),
            event_count=self.event_count + 1
        )


class CartStateAggregator:
    """
    Maintains real-time cart state per user using Faust tables.
    """

    def __init__(self, state_table) -> None:
        self._state_table = state_table
        self._logger = logging.getLogger(self.__class__.__name__)

    def _get_state_key(self, user_id: int) -> str:
        return f"cart_state:{user_id}"

    @staticmethod
    def _normalize_items(items: dict) -> Dict[int, int]:
        """Faust/JSON may persist dict keys as strings; use int keys in Python."""
        return {int(product_id): int(qty) for product_id, qty in items.items()}

    def _load_state(self, user_id: int) -> UserCartState:
        """Load user cart from Faust table or return empty cart."""
        key = self._get_state_key(user_id)
        current_data = self._state_table.get(key)
        if not current_data:
            return UserCartState(user_id=user_id)

        last_updated = current_data['last_updated']
        if isinstance(last_updated, str):
            last_updated = datetime.fromisoformat(last_updated)

        return UserCartState(
            user_id=current_data['user_id'],
            items=self._normalize_items(current_data.get('items') or {}),
            total_value=current_data['total_value'],
            last_updated=last_updated,
            event_count=current_data['event_count'],
        )

    @staticmethod
    def _apply_event(state: UserCartState, event: CartEvent) -> UserCartState:
        """Apply event to cart state without persisting."""
        if event.event_type == 'add_to_cart':
            return state.add_item(event.product_id or 0, event.quantity, event.price)
        if event.event_type == 'remove_item':
            return state.remove_item(event.product_id or 0, event.quantity, event.price)
        if event.event_type == 'update_quantity':
            return state.update_quantity(event.product_id or 0, event.quantity, event.price)
        if event.event_type == 'checkout':
            return state.checkout()
        return state

    def preview_cart_totals(self, event: CartEvent) -> tuple[float, float]:
        """Return (cart_total_before, cart_total_after) from Faust state."""
        current_state = self._load_state(event.user_id)
        new_state = self._apply_event(current_state, event)
        return current_state.total_value, new_state.total_value

    def update_cart(self, event: EnrichedCartEvent) -> UserCartState:
        """Update user cart state based on event type."""
        key = self._get_state_key(event.user_id)
        current_state = self._load_state(event.user_id)
        new_state = self._apply_event(current_state, event)

        last_updated = new_state.last_updated
        if isinstance(last_updated, str):
            last_updated = datetime.fromisoformat(last_updated)

        self._state_table[key] = {
            'user_id': new_state.user_id,
            'items': new_state.items,
            'total_value': new_state.total_value,
            'last_updated': last_updated.isoformat(),
            'event_count': new_state.event_count
        }

        return new_state

    def get_user_cart(self, user_id: int) -> Optional[UserCartState]:
        """Retrieve current cart state for user"""
        if not self._state_table.get(self._get_state_key(user_id)):
            return None
        return self._load_state(user_id)
