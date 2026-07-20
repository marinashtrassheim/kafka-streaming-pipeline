from datetime import datetime, timedelta

from app.processors.deduplicator import Deduplicator, ProcessedRecord


class TestDeduplicator:
    def test_first_event_is_not_duplicate(self, state_table, make_event):
        dedup = Deduplicator(state_table=state_table)
        event = make_event()
        assert dedup.is_duplicate(event) is False

    def test_marked_event_is_duplicate_on_next_check(self, state_table, make_event):
        dedup = Deduplicator(state_table=state_table)
        event = make_event(event_id="evt-dup")

        assert dedup.is_duplicate(event) is False
        dedup.mark_processed(event)
        assert dedup.is_duplicate(event) is True

    def test_different_event_id_same_user_is_not_duplicate(self, state_table, make_event):
        dedup = Deduplicator(state_table=state_table)
        first = make_event(event_id="evt-a")
        second = make_event(event_id="evt-b")

        dedup.mark_processed(first)
        assert dedup.is_duplicate(second) is False

    def test_expired_entry_is_no_longer_duplicate(self, state_table, make_event):
        dedup = Deduplicator(state_table=state_table, ttl_hours=1)
        event = make_event(event_id="evt-expired")

        stale_record = ProcessedRecord(
            event_id=event.event_id,
            user_id=event.user_id,
            processed_at=datetime.now() - timedelta(hours=2),
        )
        state_table[f"dedup:{event.user_id}:{event.event_id}"] = stale_record.to_dict()

        assert dedup.is_duplicate(event) is False
        # Expired key is cleaned up, not left behind.
        assert f"dedup:{event.user_id}:{event.event_id}" not in state_table

    def test_generate_event_id_is_deterministic(self, make_event):
        event = make_event()
        assert Deduplicator.generate_event_id(event) == Deduplicator.generate_event_id(event)
