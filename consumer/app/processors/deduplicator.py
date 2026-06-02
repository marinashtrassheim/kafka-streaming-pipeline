"""
Deduplication logic using Faust state stores
"""
import hashlib
from datetime import datetime, timedelta
from typing import Dict, Any
import logging
from dataclasses import dataclass

from app.schemas import CartEvent


@dataclass(frozen=True)
class ProcessedRecord:
    """Record of processed event stored in state"""
    event_id: str
    user_id: int
    processed_at: datetime
    partition: int | None = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'event_id': self.event_id,
            'user_id': self.user_id,
            'processed_at': self.processed_at.isoformat(),
            'partition': self.partition
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ProcessedRecord':
        return cls(
            event_id=data['event_id'],
            user_id=data['user_id'],
            processed_at=datetime.fromisoformat(data['processed_at']),
            partition=data.get('partition')
        )


class Deduplicator:
    """
    Event deduplication service using Faust state store.

    Implements idempotent processing by tracking processed event IDs.
    Uses TTL to prevent unbounded state growth (expired keys are removed).
    """

    def __init__(
            self,
            state_table,
            ttl_hours: int = 24,
            generate_ids: bool = False
    ) -> None:
        self._state_table = state_table
        self._ttl = timedelta(hours=ttl_hours)
        self._generate_ids = generate_ids
        self._logger = logging.getLogger(self.__class__.__name__)

    def _get_key(self, event: CartEvent) -> str:
        if self._generate_ids:
            event_id = self.generate_event_id(event)
        else:
            event_id = event.event_id

        return f"dedup:{event.user_id}:{event_id}"

    def is_duplicate(self, event: CartEvent) -> bool:
        """Check if event has been processed before (within TTL)."""
        key = self._get_key(event)
        record = self._state_table.get(key)

        if not record:
            return False

        processed_at = datetime.fromisoformat(record['processed_at'])
        if datetime.now() - processed_at < self._ttl:
            self._logger.debug("Duplicate detected: %s", key)
            return True

        del self._state_table[key]
        self._logger.debug("Expired dedup entry removed: %s", key)
        return False

    def mark_processed(self, event: CartEvent) -> None:
        """Mark event as processed in state store."""
        key = self._get_key(event)
        record = ProcessedRecord(
            event_id=event.event_id,
            user_id=event.user_id,
            processed_at=datetime.now()
        )

        self._state_table[key] = record.to_dict()
        self._logger.debug("Marked as processed: %s", key)

    def is_duplicate_batch(self, events: list[CartEvent]) -> list[bool]:
        """Check multiple events for duplicates."""
        return [self.is_duplicate(event) for event in events]

    @staticmethod
    def generate_event_id(event: CartEvent) -> str:
        """Generate deterministic event ID from event content."""
        content = (
            f"{event.user_id}:{event.event_type}:{event.product_id}:"
            f"{event.timestamp.isoformat()}"
        )
        return hashlib.md5(content.encode()).hexdigest()

    def get_stats(self) -> Dict[str, Any]:
        """Get deduplication configuration summary."""
        return {
            'ttl_hours': self._ttl.total_seconds() / 3600,
            'generate_ids': self._generate_ids
        }
