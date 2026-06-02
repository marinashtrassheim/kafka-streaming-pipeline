"""Kafka consumer lag helpers."""

import logging

import faust

logger = logging.getLogger(__name__)


async def get_consumer_lag(app: faust.App) -> int:
    """Sum lag (high watermark minus current position) for assigned partitions."""
    consumer = app.consumer
    if consumer is None:
        return 0

    assignment = consumer.assignment()
    if not assignment:
        return 0

    total_lag = 0
    for tp in assignment:
        try:
            position = await consumer.position(tp)
            highwater = consumer.highwater(tp)
        except Exception as exc:
            logger.debug("Could not read lag for %s: %s", tp, exc)
            continue

        if position is None or highwater is None:
            continue

        total_lag += max(0, highwater - position)

    return total_lag
