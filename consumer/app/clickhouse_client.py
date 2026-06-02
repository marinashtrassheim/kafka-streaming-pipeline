"""
ClickHouse client for batch inserts into schema managed by SQL migrations.
"""

import asyncio
import logging
from typing import Any, Dict, List

from clickhouse_driver import Client

logger = logging.getLogger(__name__)

_INSERT_SQL = """
    INSERT INTO cart_events_raw (
        event_id,
        user_id,
        session_id,
        event_type,
        product_id,
        product_name,
        quantity,
        price,
        timestamp,
        processed_at,
        processing_lag,
        is_late,
        hour_of_day,
        day_of_week,
        cart_total_before,
        cart_total_after
    ) VALUES
"""


class ClickHouseClient:
    """ClickHouse client with connection pooling and async-friendly inserts."""

    def __init__(self) -> None:
        from app.config import Config

        self.host = Config.CLICKHOUSE_HOST
        self.port = Config.CLICKHOUSE_PORT
        self._pool: List[Client] = []
        self._max_pool_size = 10

    def get_connection(self) -> Client:
        if self._pool:
            return self._pool.pop()
        return Client(host=self.host, port=self.port)

    def return_connection(self, conn: Client) -> None:
        if len(self._pool) < self._max_pool_size:
            self._pool.append(conn)
        else:
            conn.disconnect()

    def _insert_batch_sync(self, events: List[Dict[str, Any]]) -> None:
        if not events:
            return

        conn = self.get_connection()
        try:
            conn.execute(_INSERT_SQL, events)
            logger.debug("Inserted %s events into cart_events_raw", len(events))
        except Exception as exc:
            logger.error("ClickHouse insert failed: %s", exc)
            raise
        finally:
            self.return_connection(conn)

    async def insert_batch(self, events: List[Dict[str, Any]]) -> None:
        """Insert enriched events without blocking the asyncio event loop."""
        if not events:
            return

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._insert_batch_sync, events)

    def close(self) -> None:
        """Disconnect pooled clients on worker shutdown."""
        while self._pool:
            self._pool.pop().disconnect()
