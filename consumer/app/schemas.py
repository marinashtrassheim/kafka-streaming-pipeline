"""
Data schemas for cart events using dataclasses
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
import faust

@dataclass
class CartEvent(faust.Record):
    """Raw cart event from producer"""
    event_id: str
    user_id: int
    event_type: str # 'add_to_cart', 'remove_item', 'update_quantity', 'checkout'
    product_id: Optional[int] = None
    product_name: Optional[str] = None
    quantity: int = 1
    price: float = 0.0
    timestamp: datetime = None
    session_id: str = ""

    def __post_init__(self):
        self.timestamp = self._coerce_timestamp(self.timestamp)

    @staticmethod
    def _coerce_timestamp(value) -> datetime:
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        if value is None:
            return datetime.now()
        return value

@dataclass
class EnrichedCartEvent(CartEvent):
    """Enriched event with processing metadata"""
    processing_lag: float = 0.0  # seconds between event time and processing time
    is_late: bool = False
    hour_of_day: int = 0
    day_of_week: int = 0
    cart_total_before: float = 0.0
    cart_total_after: float = 0.0
    processed_at: datetime = field(default_factory=datetime.now)

    def to_clickhouse_row(self) -> dict:
        """Serialize event for cart_events_raw INSERT."""
        return {
            'event_id': self.event_id,
            'user_id': self.user_id,
            'session_id': self.session_id,
            'event_type': self.event_type,
            'product_id': self.product_id or 0,
            'product_name': self.product_name or '',
            'quantity': self.quantity,
            'price': self.price,
            'timestamp': self.timestamp,
            'processed_at': self.processed_at,
            'processing_lag': max(0, int(self.processing_lag)),
            'is_late': int(self.is_late),
            'hour_of_day': self.hour_of_day,
            'day_of_week': self.day_of_week,
            'cart_total_before': self.cart_total_before,
            'cart_total_after': self.cart_total_after,
        }
