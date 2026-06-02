"""Cart event payload contract (matches consumer CartEvent JSON)."""

from dataclasses import dataclass, asdict
from typing import Any, Optional


VALID_EVENT_TYPES = frozenset({
    'add_to_cart',
    'remove_item',
    'update_quantity',
    'checkout',
})


@dataclass(frozen=True)
class CartEventPayload:
    event_id: str
    user_id: int
    event_type: str
    product_id: Optional[int] = None
    product_name: Optional[str] = None
    quantity: int = 1
    price: float = 0.0
    timestamp: str = ""
    session_id: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def validate(self) -> None:
        if self.event_type not in VALID_EVENT_TYPES:
            raise ValueError(f"Invalid event_type: {self.event_type}")
        if self.event_type in {'add_to_cart', 'remove_item', 'update_quantity'}:
            if not self.product_id:
                raise ValueError(f"product_id required for {self.event_type}")
        if self.quantity <= 0 and self.event_type not in {'checkout', 'update_quantity'}:
            raise ValueError(f"Invalid quantity: {self.quantity}")
        if self.price < 0:
            raise ValueError(f"Negative price: {self.price}")
