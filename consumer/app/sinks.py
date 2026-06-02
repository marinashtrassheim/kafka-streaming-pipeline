"""Durable sinks: ClickHouse, Faust state, downstream Kafka topics."""

import logging
from typing import TYPE_CHECKING

from app.metrics import record_flush_success
from app.schemas import EnrichedCartEvent

if TYPE_CHECKING:
    from app.pipeline_state import PipelineState

logger = logging.getLogger(__name__)


async def flush_buffer_to_clickhouse(
    buffer: list[EnrichedCartEvent],
    pipeline: 'PipelineState',
    enriched_events_topic,
) -> None:
    """
    Durable write path for a batch of enriched events:
    ClickHouse -> dedup mark -> cart state -> enriched topic -> metrics.
    """
    if not buffer:
        return

    batch = list(buffer)
    try:
        await pipeline.clickhouse_client.insert_batch(
            [event.to_clickhouse_row() for event in batch]
        )

        for enriched in batch:
            pipeline.cart_processor.commit_processed(enriched)
            pipeline.cart_aggregator.update_cart(enriched)

        for enriched in batch:
            await enriched_events_topic.send(value=enriched)

        for enriched in batch:
            record_flush_success(enriched)

        logger.info("Flushed %s events to ClickHouse", len(batch))
        buffer.clear()
    except Exception:
        buffer.clear()
        raise
