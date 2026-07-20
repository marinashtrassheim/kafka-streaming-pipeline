from datetime import datetime, timedelta

from app.processors.late_event_handler import LateEventAction, LateEventHandler


class TestLateEventHandler:
    def test_first_event_for_user_is_not_late(self, state_table, make_event):
        handler = LateEventHandler(watermark_store=state_table, allowed_lateness_seconds=300)
        event = make_event(timestamp=datetime(2026, 1, 1, 12, 0, 0))

        is_late, result = handler.process(event)

        assert is_late is False
        assert result is event

    def test_in_order_event_advances_watermark(self, state_table, make_event):
        handler = LateEventHandler(watermark_store=state_table, allowed_lateness_seconds=300)
        first = make_event(event_id="e1", timestamp=datetime(2026, 1, 1, 12, 0, 0))
        second = make_event(event_id="e2", timestamp=datetime(2026, 1, 1, 12, 5, 0))

        handler.process(first)
        is_late, result = handler.process(second)

        assert is_late is False
        assert result is second

    def test_late_event_within_tolerance_is_accepted_and_flagged(self, state_table, make_event):
        handler = LateEventHandler(
            watermark_store=state_table,
            allowed_lateness_seconds=300,
            action_for_late=LateEventAction.ACCEPT,
        )
        on_time = make_event(event_id="e1", timestamp=datetime(2026, 1, 1, 12, 10, 0))
        late = make_event(event_id="e2", timestamp=datetime(2026, 1, 1, 12, 7, 0))  # 3 min late

        handler.process(on_time)
        is_late, result = handler.process(late)

        assert is_late is True
        assert result is late

    def test_late_event_beyond_tolerance_is_rejected(self, state_table, make_event):
        handler = LateEventHandler(
            watermark_store=state_table,
            allowed_lateness_seconds=300,
            action_for_late=LateEventAction.ACCEPT,
        )
        on_time = make_event(event_id="e1", timestamp=datetime(2026, 1, 1, 12, 20, 0))
        very_late = make_event(event_id="e2", timestamp=datetime(2026, 1, 1, 12, 0, 0))  # 20 min late

        handler.process(on_time)
        is_late, result = handler.process(very_late)

        assert is_late is True
        assert result is None

    def test_reject_action_rejects_even_within_tolerance(self, state_table, make_event):
        """REJECT means reject all late events outright — tolerance only
        matters for ACCEPT/ADJUST_WINDOW."""
        handler = LateEventHandler(
            watermark_store=state_table,
            allowed_lateness_seconds=300,
            action_for_late=LateEventAction.REJECT,
        )
        on_time = make_event(event_id="e1", timestamp=datetime(2026, 1, 1, 12, 10, 0))
        slightly_late = make_event(event_id="e2", timestamp=datetime(2026, 1, 1, 12, 8, 0))

        handler.process(on_time)
        is_late, result = handler.process(slightly_late)

        assert is_late is True
        assert result is None

    def test_adjust_window_corrects_timestamp_to_watermark(self, state_table, make_event):
        handler = LateEventHandler(
            watermark_store=state_table,
            allowed_lateness_seconds=300,
            action_for_late=LateEventAction.ADJUST_WINDOW,
        )
        watermark_ts = datetime(2026, 1, 1, 12, 10, 0)
        on_time = make_event(event_id="e1", timestamp=watermark_ts)
        late = make_event(event_id="e2", timestamp=datetime(2026, 1, 1, 12, 7, 0))

        handler.process(on_time)
        is_late, result = handler.process(late)

        assert is_late is True
        assert result is not None
        assert result.timestamp == watermark_ts
        assert result.event_id == late.event_id
