"""Dead-letter queue publishing for failed or rejected events."""

from datetime import datetime
from typing import Any, Mapping

from app.schemas import CartEvent


async def publish_dead_letter(
    topic,
    *,
    original_event: Mapping[str, Any],
    error: str,
) -> None:
    """Send a failed event payload to the dead-letter topic."""
    await topic.send(
        value={
            'original_event': original_event,
            'error': error,
            'timestamp': datetime.now().isoformat(),
        }
    )


async def publish_late_event_rejected(topic, event: CartEvent) -> None:
    await publish_dead_letter(
        topic,
        original_event=event.asdict(),
        error='late_event_beyond_tolerance',
    )


async def publish_validation_error(topic, event: CartEvent, exc: ValueError) -> None:
    await publish_dead_letter(
        topic,
        original_event=event.asdict(),
        error=f'validation_error: {exc}',
    )


async def publish_unexpected_error(topic, event: CartEvent, exc: Exception) -> None:
    await publish_dead_letter(
        topic,
        original_event=event.asdict(),
        error=f'unexpected_error: {exc}',
    )


async def publish_flush_failed(topic, event: CartEvent, exc: Exception) -> None:
    """Used when a ClickHouse batch flush fails — called once per event in the
    failed batch, not just the event that happened to trigger the flush."""
    await publish_dead_letter(
        topic,
        original_event=event.asdict(),
        error=f'clickhouse_flush_failed: {exc}',
    )
