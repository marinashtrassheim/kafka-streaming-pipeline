"""Prometheus metrics for the cart processor worker."""

from prometheus_client import Counter, Histogram, Gauge

EVENTS_PROCESSED = Counter(
    'events_processed_total',
    'Total events processed',
    ['event_type', 'status'],
)
PROCESSING_TIME = Histogram(
    'event_processing_seconds',
    'Event processing time in seconds',
)
CONSUMER_LAG = Gauge(
    'consumer_lag',
    'Current consumer lag per partition',
)
DUPLICATE_EVENTS = Counter(
    'duplicate_events_total',
    'Total duplicate events detected',
)
LATE_EVENTS = Counter(
    'late_events_total',
    'Total late events detected',
    ['action'],
)


def record_flush_success(enriched) -> None:
    """Record metrics after a successful durable flush."""
    EVENTS_PROCESSED.labels(
        event_type=enriched.event_type,
        status='success',
    ).inc()
    if enriched.is_late:
        LATE_EVENTS.labels(action='accepted').inc()
