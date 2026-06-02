"""
Handle out-of-order events with watermarks
"""

from datetime import datetime, timedelta
from typing import Tuple, Optional, Dict, Any
import logging
from dataclasses import dataclass
from enum import Enum

from app.schemas import CartEvent


class LateEventAction(Enum):
    """Action to take for late event"""
    ACCEPT = "accept"
    REJECT = "reject"
    ADJUST_WINDOW = "adjust_window"


@dataclass(frozen=True)
class WatermarkState:
    """Immutable watermark state for a user"""
    max_timestamp: datetime
    last_processed_at: datetime
    late_event_count: int = 0
    adjusted_event_count: int = 0

    def update(self, new_timestamp: datetime) -> 'WatermarkState':
        """Create new state with updated watermark"""
        return WatermarkState(
            max_timestamp=max(self.max_timestamp, new_timestamp),
            last_processed_at=datetime.now(),
            late_event_count=self.late_event_count,
            adjusted_event_count=self.adjusted_event_count
        )

    def record_late_event(self) -> 'WatermarkState':
        """Create new state with late event count incremented"""
        return WatermarkState(
            max_timestamp=self.max_timestamp,
            last_processed_at=self.last_processed_at,
            late_event_count=self.late_event_count + 1,
            adjusted_event_count=self.adjusted_event_count
        )

    def record_adjusted(self) -> 'WatermarkState':
        """Create new state with adjusted event count incremented"""
        return WatermarkState(
            max_timestamp=self.max_timestamp,
            last_processed_at=self.last_processed_at,
            late_event_count=self.late_event_count,
            adjusted_event_count=self.adjusted_event_count + 1
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            'max_timestamp': self.max_timestamp.isoformat(),
            'last_processed_at': self.last_processed_at.isoformat(),
            'late_event_count': self.late_event_count,
            'adjusted_event_count': self.adjusted_event_count
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WatermarkState':
        return cls(
            max_timestamp=datetime.fromisoformat(data['max_timestamp']),
            last_processed_at=datetime.fromisoformat(data['last_processed_at']),
            late_event_count=data.get('late_event_count', 0),
            adjusted_event_count=data.get('adjusted_event_count', 0)
        )

    @classmethod
    def initial(cls, user_id: int) -> 'WatermarkState':
        return cls(
            max_timestamp=datetime(1970, 1, 1),  # Unix epoch start
            last_processed_at=datetime.now(),
            late_event_count=0,
            adjusted_event_count=0
        )


@dataclass(frozen=True)
class LateEventResult:
    """Result of late event processing"""
    is_late: bool
    action: LateEventAction
    corrected_event: Optional[CartEvent]
    lateness_seconds: float
    watermark_before: datetime
    watermark_after: datetime


class LateEventHandler:
    """
    Handles out-of-order events using per-user watermarks.

    Implements the standard streaming watermark pattern:
    - Track max timestamp seen per user
    - Events older than watermark are "late"
    - Configurable tolerance for acceptable lateness
    """

    def __init__(
            self,
            watermark_store,
            allowed_lateness_seconds: int = 300,
            action_for_late: LateEventAction = LateEventAction.ACCEPT
    ) -> None:
        """
        Initialize late event handler.

        Args:
            watermark_store: Faust Table for watermark state
            allowed_lateness_seconds: Maximum allowed lateness (default 5 min)
            action_for_late: What to do with late events
        """
        self._watermark_store = watermark_store
        self._allowed_lateness = timedelta(seconds=allowed_lateness_seconds)
        self._action = action_for_late
        self._logger = logging.getLogger(self.__class__.__name__)

    def _get_user_key(self, user_id: int) -> str:
        """Generate state store key for user watermark"""
        return f"watermark:{user_id}"

    def _get_watermark(self, user_id: int) -> WatermarkState:
        """Get current watermark state for user"""
        key = self._get_user_key(user_id)
        data = self._watermark_store.get(key)

        if data:
            return WatermarkState.from_dict(data)

        return WatermarkState.initial(user_id)

    def _set_watermark(self, user_id: int, state: WatermarkState) -> None:
        """Persist watermark state for user"""
        key = self._get_user_key(user_id)
        self._watermark_store[key] = state.to_dict()

    def process(self, event: CartEvent) -> Tuple[bool, Optional[CartEvent]]:
        """
        Process event, handling out-of-order arrival.
        """
        event.timestamp = CartEvent._coerce_timestamp(event.timestamp)

        watermark_state = self._get_watermark(event.user_id)
        current_watermark = watermark_state.max_timestamp

        # Calculate lateness
        if event.timestamp < current_watermark:
            lateness = (current_watermark - event.timestamp).total_seconds()
            is_late = True

            self._logger.info(
                f"Late event for user {event.user_id}: "
                f"event_time={event.timestamp.isoformat()}, "
                f"watermark={current_watermark.isoformat()}, "
                f"lateness={lateness:.2f}s"
            )

            # Check if lateness is within tolerance
            if lateness <= self._allowed_lateness.total_seconds():
                self._logger.debug(f"Late event within tolerance: {lateness}s")

                if self._action == LateEventAction.ACCEPT:
                    new_watermark = watermark_state.record_late_event()
                    self._set_watermark(event.user_id, new_watermark)
                    return True, event

                elif self._action == LateEventAction.ADJUST_WINDOW:
                    # Adjust event timestamp to watermark for windowing
                    corrected_event = self._adjust_timestamp(event, current_watermark)
                    new_watermark = watermark_state.record_adjusted()
                    self._set_watermark(event.user_id, new_watermark)
                    return True, corrected_event

            # Too late or action is reject
            new_watermark = watermark_state.record_late_event()
            self._set_watermark(event.user_id, new_watermark)
            return True, None

        # Event is in order
        new_watermark = watermark_state.update(event.timestamp)
        self._set_watermark(event.user_id, new_watermark)

        return False, event

    def _adjust_timestamp(self, event: CartEvent, watermark: datetime) -> CartEvent:
        """
        Adjust event timestamp to watermark for window aggregation.
        Preserves original timestamp in a separate field for auditing.
        """
        # Create a copy with adjusted timestamp
        adjusted_event = CartEvent(
            event_id=event.event_id,
            user_id=event.user_id,
            event_type=event.event_type,
            product_id=event.product_id,
            product_name=event.product_name,
            quantity=event.quantity,
            price=event.price,
            timestamp=watermark,  # Adjusted for windowing
            session_id=event.session_id
        )

        # Add original timestamp as metadata if schema allows
        adjusted_event.original_timestamp = event.timestamp

        return adjusted_event

    def get_watermark_stats(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Get watermark statistics for a user"""
        state = self._get_watermark(user_id)

        return {
            'max_timestamp': state.max_timestamp.isoformat(),
            'last_processed': state.last_processed_at.isoformat(),
            'late_event_count': state.late_event_count,
            'adjusted_event_count': state.adjusted_event_count
        }