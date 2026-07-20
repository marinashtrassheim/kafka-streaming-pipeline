from app.processors.cart_state import CartStateAggregator
from app.schemas import EnrichedCartEvent


def _enriched(event_type, product_id, quantity, price, user_id=1001):
    return EnrichedCartEvent(
        event_id=f"evt-{event_type}-{product_id}",
        user_id=user_id,
        event_type=event_type,
        product_id=product_id,
        product_name="Widget",
        quantity=quantity,
        price=price,
    )


class TestCartStateAggregator:
    def test_add_item_increases_total(self, state_table):
        aggregator = CartStateAggregator(state_table=state_table)
        new_state = aggregator.update_cart(_enriched("add_to_cart", 101, 2, 10.0))

        assert new_state.total_value == 20.0
        assert new_state.items[101] == 2

    def test_add_item_twice_accumulates_quantity(self, state_table):
        aggregator = CartStateAggregator(state_table=state_table)
        aggregator.update_cart(_enriched("add_to_cart", 101, 1, 10.0))
        new_state = aggregator.update_cart(_enriched("add_to_cart", 101, 2, 10.0))

        assert new_state.items[101] == 3
        assert new_state.total_value == 30.0

    def test_remove_item_decreases_total(self, state_table):
        aggregator = CartStateAggregator(state_table=state_table)
        aggregator.update_cart(_enriched("add_to_cart", 101, 3, 10.0))
        new_state = aggregator.update_cart(_enriched("remove_item", 101, 1, 10.0))

        assert new_state.items[101] == 2
        assert new_state.total_value == 20.0

    def test_remove_more_than_available_clamps_at_zero(self, state_table):
        aggregator = CartStateAggregator(state_table=state_table)
        aggregator.update_cart(_enriched("add_to_cart", 101, 2, 10.0))
        new_state = aggregator.update_cart(_enriched("remove_item", 101, 99, 10.0))

        assert 101 not in new_state.items
        assert new_state.total_value == 0.0

    def test_update_quantity_sets_exact_value(self, state_table):
        aggregator = CartStateAggregator(state_table=state_table)
        aggregator.update_cart(_enriched("add_to_cart", 101, 1, 10.0))
        new_state = aggregator.update_cart(_enriched("update_quantity", 101, 5, 10.0))

        assert new_state.items[101] == 5
        assert new_state.total_value == 50.0

    def test_update_quantity_zero_removes_item(self, state_table):
        aggregator = CartStateAggregator(state_table=state_table)
        aggregator.update_cart(_enriched("add_to_cart", 101, 2, 10.0))
        new_state = aggregator.update_cart(_enriched("update_quantity", 101, 0, 10.0))

        assert 101 not in new_state.items
        assert new_state.total_value == 0.0

    def test_checkout_clears_cart(self, state_table):
        aggregator = CartStateAggregator(state_table=state_table)
        aggregator.update_cart(_enriched("add_to_cart", 101, 2, 10.0))
        new_state = aggregator.update_cart(_enriched("checkout", 0, 0, 0.0))

        assert new_state.items == {}
        assert new_state.total_value == 0.0

    def test_preview_cart_totals_does_not_persist(self, state_table):
        aggregator = CartStateAggregator(state_table=state_table)
        before, after = aggregator.preview_cart_totals(_enriched("add_to_cart", 101, 1, 10.0))

        assert before == 0.0
        assert after == 10.0
        assert aggregator.get_user_cart(1001) is None
