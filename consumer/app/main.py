# consumer/app/main.py
"""
Faust application: cart event processing pipeline.
"""

import logging

import faust

from app.config import Config
from app.schemas import CartEvent, EnrichedCartEvent
from app.pipeline_state import PipelineState, build_pipeline_state
from app.metrics_kafka import get_consumer_lag
from app.metrics import (
    PROCESSING_TIME,
    CONSUMER_LAG,
    DUPLICATE_EVENTS,
    EVENTS_PROCESSED,
    LATE_EVENTS,
)
from app.dead_letter import (
    publish_late_event_rejected,
    publish_validation_error,
    publish_unexpected_error,
)
from app.sinks import flush_buffer_to_clickhouse
from app.metrics_http import prometheus_blueprint

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = faust.App(
    'cart_processor',
    broker=Config.kafka_broker(),
    value_serializer='json',
    store=Config.FAUST_STATE_STORE,
    datadir=Config.FAUST_DATADIR,
    topic_partitions=6,
    consumer_auto_offset_reset='earliest',
    consumer_enable_auto_commit=False,
)

raw_events_topic = app.topic(
    'raw_events',
    value_type=CartEvent,
    partitions=6,
    retention=Config.TOPIC_RETENTION_HOURS
)

enriched_events_topic = app.topic(
    'enriched_events',
    value_type=EnrichedCartEvent,
    partitions=6
)

dead_letter_topic = app.topic('dead_letter', partitions=3)

prometheus_blueprint.register(app, url_prefix='/')

dedup_store = app.Table(
    'deduplication_store',
    default=lambda: None,
    help="Store processed event IDs",
    partitions=6,
    options=Config.FAUST_TABLE_STORE_OPTIONS,
)

watermark_store = app.Table(
    'user_watermarks',
    default=lambda: None,
    help="Per-user watermark tracking for out-of-order handling",
    partitions=6,
    options=Config.FAUST_TABLE_STORE_OPTIONS,
)

cart_state_store = app.Table(
    'user_carts',
    default=lambda: None,
    help="Current cart state per user",
    partitions=6,
    options=Config.FAUST_TABLE_STORE_OPTIONS,
)

# Init before agents start — @app.task on_startup races with @app.agent.
pipeline: PipelineState = build_pipeline_state(
    dedup_store=dedup_store,
    watermark_store=watermark_store,
    cart_state_store=cart_state_store,
)


@app.on_before_shutdown.connect
async def on_shutdown(app: faust.App, **kwargs) -> None:
    """Release ClickHouse connections."""
    pipeline.clickhouse_client.close()
    logger.info("Shutdown complete")


@app.agent(raw_events_topic)
async def process_cart_events(stream: faust.Stream) -> None:
    """
    Main processing agent.

    Flow:
    1. Validate, dedup check, watermark, enrich (read-only cart preview)
    2. Buffer enriched events
    3. On flush: see app.sinks.flush_buffer_to_clickhouse
    """
    buffer: list[EnrichedCartEvent] = []
    batch_size = Config.BATCH_SIZE

    async for event in stream.group_by(
        lambda e: str(e.user_id),
        name='user_id',
    ):
        with PROCESSING_TIME.time():
            try:
                result = pipeline.cart_processor.process(event)

                if result.is_duplicate:
                    DUPLICATE_EVENTS.inc()
                    logger.debug("Duplicate event skipped: %s", event.event_id)
                    continue

                if result.is_late and not result.enriched_event:
                    LATE_EVENTS.labels(action='rejected').inc()
                    await publish_late_event_rejected(dead_letter_topic, event)
                    continue

                enriched = result.enriched_event
                if enriched:
                    buffer.append(enriched)

                    if len(buffer) >= batch_size:
                        await flush_buffer_to_clickhouse(
                            buffer,
                            pipeline,
                            enriched_events_topic,
                        )

            except ValueError as exc:
                logger.error("Validation error for event %s: %s", event.event_id, exc)
                await publish_validation_error(dead_letter_topic, event, exc)
                EVENTS_PROCESSED.labels(
                    event_type=getattr(event, 'event_type', 'unknown'),
                    status='validation_error'
                ).inc()

            except Exception as exc:
                buffer.clear()
                logger.exception(
                    "Unexpected error processing event %s: %s",
                    event.event_id,
                    exc,
                )
                await publish_unexpected_error(dead_letter_topic, event, exc)
                EVENTS_PROCESSED.labels(
                    event_type=getattr(event, 'event_type', 'unknown'),
                    status='error'
                ).inc()

    if buffer:
        await flush_buffer_to_clickhouse(
            buffer,
            pipeline,
            enriched_events_topic,
        )


@app.timer(interval=30.0)
async def report_metrics() -> None:
    """Report Kafka consumer lag to Prometheus."""
    lag = await get_consumer_lag(app)
    CONSUMER_LAG.set(lag)
    logger.debug("Metrics reported: consumer_lag=%s", lag)


if __name__ == '__main__':
    app.main()
