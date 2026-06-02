"""Application dependencies for the cart processing pipeline."""

from dataclasses import dataclass

from app.clickhouse_client import ClickHouseClient
from app.processors.cart_processor import CartProcessor
from app.processors.cart_state import CartStateAggregator
from app.processors.deduplicator import Deduplicator
from app.processors.late_event_handler import LateEventHandler, LateEventAction
from app.config import Config


@dataclass
class PipelineState:
    """Holds wired processors and clients (initialized once per worker)."""

    cart_processor: CartProcessor
    cart_aggregator: CartStateAggregator
    clickhouse_client: ClickHouseClient


def build_pipeline_state(
    dedup_store,
    watermark_store,
    cart_state_store,
) -> PipelineState:
    deduplicator = Deduplicator(
        state_table=dedup_store,
        ttl_hours=24,
        generate_ids=False,
    )

    late_handler = LateEventHandler(
        watermark_store=watermark_store,
        allowed_lateness_seconds=Config.ALLOWED_LATENESS_SECONDS,
        action_for_late=LateEventAction.ACCEPT,
    )

    cart_aggregator = CartStateAggregator(state_table=cart_state_store)

    cart_processor = CartProcessor(
        deduplicator=deduplicator,
        late_handler=late_handler,
        cart_aggregator=cart_aggregator,
    )

    return PipelineState(
        cart_processor=cart_processor,
        cart_aggregator=cart_aggregator,
        clickhouse_client=ClickHouseClient(),
    )
