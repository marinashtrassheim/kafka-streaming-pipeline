import pytest

from app.processors.cart_processor import CartProcessor
from app.processors.cart_state import CartStateAggregator
from app.processors.deduplicator import Deduplicator
from app.processors.late_event_handler import LateEventAction, LateEventHandler


@pytest.fixture
def processor():
    return CartProcessor(
        deduplicator=Deduplicator(state_table={}),
        late_handler=LateEventHandler(
            watermark_store={},
            allowed_lateness_seconds=300,
            action_for_late=LateEventAction.ACCEPT,
        ),
        cart_aggregator=CartStateAggregator(state_table={}),
    )


class TestCartProcessorValidation:
    def test_empty_event_id_raises(self, processor, make_event):
        event = make_event(event_id="")
        with pytest.raises(ValueError, match="event_id"):
            processor.process(event)

    def test_non_positive_user_id_raises(self, processor, make_event):
        event = make_event(user_id=0)
        with pytest.raises(ValueError, match="user_id"):
            processor.process(event)

    def test_invalid_event_type_raises(self, processor, make_event):
        event = make_event(event_type="not_a_real_type")
        with pytest.raises(ValueError, match="event_type"):
            processor.process(event)

    def test_missing_product_id_for_add_to_cart_raises(self, processor, make_event):
        event = make_event(event_type="add_to_cart", product_id=None)
        with pytest.raises(ValueError, match="product_id"):
            processor.process(event)

    def test_checkout_without_product_id_is_valid(self, processor, make_event):
        event = make_event(event_type="checkout", product_id=None, quantity=0)
        result = processor.process(event)
        assert result.enriched_event is not None

    def test_negative_price_raises(self, processor, make_event):
        event = make_event(price=-1.0)
        with pytest.raises(ValueError, match="price"):
            processor.process(event)


class TestCartProcessorFlow:
    def test_valid_event_is_enriched(self, processor, make_event):
        event = make_event()
        result = processor.process(event)

        assert result.is_duplicate is False
        assert result.is_late is False
        assert result.enriched_event is not None
        assert result.enriched_event.event_id == event.event_id

    def test_duplicate_event_is_flagged_and_not_enriched(self, processor, make_event):
        event = make_event(event_id="evt-dup")

        first = processor.process(event)
        processor.commit_processed(event)
        second = processor.process(make_event(event_id="evt-dup"))

        assert first.is_duplicate is False
        assert second.is_duplicate is True
        assert second.enriched_event is None
