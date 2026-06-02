"""
Scenario producer: reproducible Kafka load for the cart streaming pipeline.

Runs for a fixed duration, publishes planned events each tick, then exits.
"""

import logging
import sys
import time
from datetime import datetime, timezone

from app.config import Config
from app.generator import create_producer, publish
from app.scenarios import events_by_second, SCENARIO_EVENTS

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)


def sleep_until_next_tick(start_monotonic: float, tick: int, tick_seconds: float) -> None:
    target = start_monotonic + (tick + 1) * tick_seconds
    delay = target - time.monotonic()
    if delay > 0:
        time.sleep(delay)


def run() -> int:
    config = Config()
    grouped = events_by_second()
    producer = create_producer(config)
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')

    logger.info(
        "Starting scenario producer: run_id=%s duration=%ss, tick=%ss, topic=%s, events_planned=%s",
        run_id,
        config.RUN_DURATION_SECONDS,
        config.TICK_SECONDS,
        config.KAFKA_TOPIC,
        len(SCENARIO_EVENTS),
    )

    sent_count = 0
    start = time.monotonic()

    try:
        for tick in range(config.RUN_DURATION_SECONDS):
            for spec in grouped.get(tick, []):
                payload = publish(producer, config, spec, run_id)
                sent_count += 1
                logger.info(
                    "SENT tick=%s scenario=%s event_id=%s user_id=%s type=%s ts=%s",
                    tick,
                    spec.scenario,
                    payload.event_id,
                    payload.user_id,
                    payload.event_type,
                    payload.timestamp,
                )

            sleep_until_next_tick(start, tick, config.TICK_SECONDS)

        producer.flush()
        logger.info(
            "Scenario run complete: ticks=%s, events_sent=%s",
            config.RUN_DURATION_SECONDS,
            sent_count,
        )
        return 0
    except Exception:
        logger.exception("Scenario producer failed")
        return 1
    finally:
        producer.close()


if __name__ == '__main__':
    sys.exit(run())
