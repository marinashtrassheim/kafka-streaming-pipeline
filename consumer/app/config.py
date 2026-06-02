"""Application configuration"""

import os
from datetime import timedelta


class Config:
    """Centralized configuration management"""

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

    # Faust — store URL without path: each Table gets its own RocksDB files
    FAUST_STATE_STORE = os.getenv('FAUST_STATE_STORE', 'rocksdb://')
    FAUST_DATADIR = os.getenv('FAUST_DATADIR', '/app/faust-data')
    FAUST_ROCKSDB_DRIVER = os.getenv('FAUST_ROCKSDB_DRIVER', 'rocksdict')
    FAUST_TABLE_STORE_OPTIONS = {
        'driver': FAUST_ROCKSDB_DRIVER,
    }

    # ClickHouse
    CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
    CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', '9000'))

    # Processing
    ALLOWED_LATENESS_SECONDS = int(os.getenv('ALLOWED_LATENESS_SECONDS', '300'))
    TOPIC_RETENTION_HOURS = timedelta(hours=int(os.getenv('TOPIC_RETENTION_HOURS', '168')))

    # Batch settings
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '10000'))

    @classmethod
    def kafka_broker(cls) -> str:
        """Return Faust-compatible Kafka broker URL."""
        broker = cls.KAFKA_BOOTSTRAP_SERVERS
        if '://' not in broker:
            return f'kafka://{broker}'
        return broker
