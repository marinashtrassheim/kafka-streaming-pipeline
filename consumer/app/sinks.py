"""Durable sinks: ClickHouse, Faust state, downstream Kafka topics."""

import logging
from typing import TYPE_CHECKING

from app.dead_letter import publish_flush_failed
from app.metrics import record_flush_success
from app.schemas import EnrichedCartEvent

if TYPE_CHECKING:
    from app.pipeline_state import PipelineState

logger = logging.getLogger(__name__)


class FlushError(Exception):
    """Raised when a ClickHouse flush fails. By the time this is raised, every
    event in the batch has already been dead-lettered (not just the one event
    that happened to trigger the flush) — callers don't need to dead-letter
    again, only log/record metrics."""


async def flush_buffer_to_clickhouse(
    buffer: list[EnrichedCartEvent],
    pipeline: 'PipelineState',
    enriched_events_topic,
    dead_letter_topic,
) -> None:
    """
    Durable write path for a batch of enriched events:
    ClickHouse -> dedup mark -> cart state -> enriched topic -> metrics.

    If the ClickHouse insert fails, every event in the batch is dead-lettered
    before raising FlushError — previously only the single event that
    triggered the flush was dead-lettered and the rest of the batch (up to
    BATCH_SIZE - 1 events) was silently dropped.
    """
    if not buffer:
        return

    batch = list(buffer)
    buffer.clear()

    try:
        await pipeline.clickhouse_client.insert_batch(
            [event.to_clickhouse_row() for event in batch]
        )
    except Exception as exc:
        logger.error("ClickHouse flush failed for %s event(s): %s", len(batch), exc)
        for enriched in batch:
            await publish_flush_failed(dead_letter_topic, enriched, exc)
        raise FlushError(f"ClickHouse flush failed for {len(batch)} event(s)") from exc

    for enriched in batch:
        pipeline.cart_processor.commit_processed(enriched)
        pipeline.cart_aggregator.update_cart(enriched)

    for enriched in batch:
        await enriched_events_topic.send(value=enriched)

    for enriched in batch:
        record_flush_success(enriched)

    logger.info("Flushed %s events to ClickHouse", len(batch))
