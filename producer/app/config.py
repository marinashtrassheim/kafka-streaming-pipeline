"""Producer configuration from environment."""

import os


class Config:
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'raw_events')
    RUN_DURATION_SECONDS = int(os.getenv('SCENARIO_RUN_DURATION_SECONDS', '120'))
    TICK_SECONDS = float(os.getenv('SCENARIO_TICK_SECONDS', '1'))
