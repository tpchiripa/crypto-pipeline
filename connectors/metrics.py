"""
Shared metrics definitions for connectors. Every connector, regardless of
source, reports the same three signals: how many events it published,
how many times it failed to, and when it last successfully published -
that last one lets you alert on "this connector went quiet" even when it
isn't throwing errors, which is a common silent-failure mode (e.g. a
WebSocket that connects but stops receiving messages).
"""
from prometheus_client import Counter, Gauge, start_http_server

EVENTS_PUBLISHED = Counter(
    "connector_events_published_total",
    "Number of events successfully published to Kafka",
    ["source_id"],
)

PUBLISH_ERRORS = Counter(
    "connector_publish_errors_total",
    "Number of errors encountered while publishing or streaming",
    ["source_id"],
)

LAST_EVENT_TIMESTAMP = Gauge(
    "connector_last_event_timestamp_seconds",
    "Unix timestamp of the last successfully published event",
    ["source_id"],
)


def start_metrics_server(port: int) -> None:
    start_http_server(port)
