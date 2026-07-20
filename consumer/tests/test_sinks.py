"""Regression tests for the batch-flush data-loss fix.

Previously, a ClickHouse insert failure cleared the whole buffer but only
dead-lettered the single event that triggered the flush — every other event
in the batch (up to BATCH_SIZE - 1) was silently dropped.
"""
from dataclasses import dataclass, field

import pytest

from app.sinks import FlushError, flush_buffer_to_clickhouse


@dataclass
class FakeTopic:
    sent: list = field(default_factory=list)

    async def send(self, value):
        self.sent.append(value)


class FakeClickHouseClient:
    def __init__(self, should_fail: bool = False):
        self.should_fail = should_fail
        self.inserted_batches = []

    async def insert_batch(self, rows):
        if self.should_fail:
            raise RuntimeError("clickhouse unavailable")
        self.inserted_batches.append(rows)


@dataclass
class FakeCartProcessor:
    committed: list = field(default_factory=list)

    def commit_processed(self, event):
        self.committed.append(event)


@dataclass
class FakeCartAggregator:
    updated: list = field(default_factory=list)

    def update_cart(self, event):
        self.updated.append(event)


@dataclass
class FakePipeline:
    clickhouse_client: FakeClickHouseClient
    cart_processor: FakeCartProcessor = field(default_factory=FakeCartProcessor)
    cart_aggregator: FakeCartAggregator = field(default_factory=FakeCartAggregator)


def _enriched(event_id, user_id=1001):
    from app.schemas import EnrichedCartEvent
    return EnrichedCartEvent(
        event_id=event_id,
        user_id=user_id,
        event_type="add_to_cart",
        product_id=101,
        product_name="Widget",
        quantity=1,
        price=9.99,
    )


class TestFlushSuccess:
    async def test_successful_flush_inserts_commits_and_publishes_all_events(self):
        batch = [_enriched("e1"), _enriched("e2"), _enriched("e3")]
        buffer = list(batch)
        pipeline = FakePipeline(clickhouse_client=FakeClickHouseClient(should_fail=False))
        enriched_topic = FakeTopic()
        dead_letter_topic = FakeTopic()

        await flush_buffer_to_clickhouse(buffer, pipeline, enriched_topic, dead_letter_topic)

        assert len(pipeline.clickhouse_client.inserted_batches[0]) == 3
        assert len(pipeline.cart_processor.committed) == 3
        assert len(pipeline.cart_aggregator.updated) == 3
        assert len(enriched_topic.sent) == 3
        assert dead_letter_topic.sent == []
        assert buffer == []


class TestFlushFailureDeadLettersWholeBatch:
    async def test_failed_flush_dead_letters_every_event_not_just_one(self):
        batch = [_enriched("e1"), _enriched("e2"), _enriched("e3")]
        buffer = list(batch)
        pipeline = FakePipeline(clickhouse_client=FakeClickHouseClient(should_fail=True))
        enriched_topic = FakeTopic()
        dead_letter_topic = FakeTopic()

        with pytest.raises(FlushError):
            await flush_buffer_to_clickhouse(buffer, pipeline, enriched_topic, dead_letter_topic)

        # All 3 events dead-lettered, not just the one that triggered the flush.
        assert len(dead_letter_topic.sent) == 3
        dead_lettered_ids = {msg['original_event']['event_id'] for msg in dead_letter_topic.sent}
        assert dead_lettered_ids == {"e1", "e2", "e3"}

    async def test_failed_flush_does_not_commit_dedup_or_cart_state(self):
        """Nothing should be marked processed for events that never made it
        into ClickHouse — otherwise a retry would treat them as duplicates."""
        batch = [_enriched("e1"), _enriched("e2")]
        buffer = list(batch)
        pipeline = FakePipeline(clickhouse_client=FakeClickHouseClient(should_fail=True))
        enriched_topic = FakeTopic()
        dead_letter_topic = FakeTopic()

        with pytest.raises(FlushError):
            await flush_buffer_to_clickhouse(buffer, pipeline, enriched_topic, dead_letter_topic)

        assert pipeline.cart_processor.committed == []
        assert pipeline.cart_aggregator.updated == []
        assert enriched_topic.sent == []

    async def test_failed_flush_still_clears_buffer(self):
        batch = [_enriched("e1")]
        buffer = list(batch)
        pipeline = FakePipeline(clickhouse_client=FakeClickHouseClient(should_fail=True))

        with pytest.raises(FlushError):
            await flush_buffer_to_clickhouse(buffer, pipeline, FakeTopic(), FakeTopic())

        assert buffer == []


class TestEmptyBuffer:
    async def test_empty_buffer_is_a_no_op(self):
        pipeline = FakePipeline(clickhouse_client=FakeClickHouseClient())
        await flush_buffer_to_clickhouse([], pipeline, FakeTopic(), FakeTopic())
        assert pipeline.clickhouse_client.inserted_batches == []
