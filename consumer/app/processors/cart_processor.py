"""Event processing logic"""

from datetime import datetime
from typing import Optional
import logging
from dataclasses import dataclass

from app.processors.deduplicator import Deduplicator
from app.processors.late_event_handler import LateEventHandler
from app.processors.cart_state import CartStateAggregator
from app.schemas import CartEvent, EnrichedCartEvent

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProcessingResult:
    """Immutable result of event processing"""
    enriched_event: Optional[EnrichedCartEvent]
    is_duplicate: bool
    is_late: bool
    processing_time_ms: float


class CartProcessor:
    """
    Main event processor orchestrating deduplication,
    out-of-order handling, and enrichment.
    """

    def __init__(
        self,
        deduplicator: Deduplicator,
        late_handler: LateEventHandler,
        cart_aggregator: CartStateAggregator,
    ) -> None:
        self._deduplicator = deduplicator
        self._late_handler = late_handler
        self._cart_aggregator = cart_aggregator
        self._logger = logger.getChild(self.__class__.__name__)

    def process(self, event: CartEvent) -> ProcessingResult:
        start_time = datetime.now()

        event.timestamp = CartEvent._coerce_timestamp(event.timestamp)

        self._validate_event(event)

        if self._deduplicator.is_duplicate(event):
            self._logger.debug("Duplicate event rejected: %s", event.event_id)
            return ProcessingResult(
                enriched_event=None,
                is_duplicate=True,
                is_late=False,
                processing_time_ms=self._elapsed_ms(start_time)
            )

        is_late, corrected_event = self._late_handler.process(event)

        if is_late and not corrected_event:
            self._logger.info("Late event beyond tolerance: %s", event.event_id)
            return ProcessingResult(
                enriched_event=None,
                is_duplicate=False,
                is_late=True,
                processing_time_ms=self._elapsed_ms(start_time)
            )

        enriched = self._enrich_event(corrected_event or event, is_late)

        return ProcessingResult(
            enriched_event=enriched,
            is_duplicate=False,
            is_late=is_late,
            processing_time_ms=self._elapsed_ms(start_time)
        )

    def commit_processed(self, event: CartEvent) -> None:
        """Mark event as processed in dedup store after durable ClickHouse write."""
        self._deduplicator.mark_processed(event)

    def _validate_event(self, event: CartEvent) -> None:
        if not event.event_id:
            raise ValueError("event_id cannot be empty")
        if event.user_id <= 0:
            raise ValueError(f"Invalid user_id: {event.user_id}")

        valid_types = {'add_to_cart', 'remove_item', 'update_quantity', 'checkout'}
        if event.event_type not in valid_types:
            raise ValueError(
                f"Invalid event_type: {event.event_type}. "
                f"Must be one of: {valid_types}"
            )

        if event.event_type in {'add_to_cart', 'remove_item', 'update_quantity'}:
            if not event.product_id:
                raise ValueError(f"product_id required for {event.event_type}")

        if event.quantity <= 0 and event.event_type not in {'checkout', 'update_quantity'}:
            raise ValueError(f"Invalid quantity: {event.quantity}")

        if event.price < 0:
            raise ValueError(f"Negative price: {event.price}")

    def _enrich_event(self, event: CartEvent, is_late: bool) -> EnrichedCartEvent:
        current_time = datetime.now()
        cart_before, cart_after = self._cart_aggregator.preview_cart_totals(event)

        return EnrichedCartEvent(
            event_id=event.event_id,
            user_id=event.user_id,
            event_type=event.event_type,
            product_id=event.product_id,
            product_name=event.product_name,
            quantity=event.quantity,
            price=event.price,
            timestamp=event.timestamp,
            session_id=event.session_id,
            processing_lag=(current_time - event.timestamp).total_seconds(),
            is_late=is_late,
            hour_of_day=event.timestamp.hour,
            day_of_week=event.timestamp.weekday(),
            cart_total_before=cart_before,
            cart_total_after=cart_after,
            processed_at=current_time
        )

    @staticmethod
    def _elapsed_ms(start_time: datetime) -> float:
        return (datetime.now() - start_time).total_seconds() * 1000
