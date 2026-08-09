"""
Normalizer service.

Consumes raw events from Kafka, writes an untouched copy to raw_events
(for replay/audit), then runs source-specific parsing rules to populate
clean, typed tables that the API and downstream consumers actually use.

To add a new source's normalization logic: add a new `parse_<source>()`
function and register it in PARSERS. The Kafka consumption loop and
Postgres writing code never change.
"""
import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable

import psycopg2
import psycopg2.extras
from aiokafka import AIOKafkaConsumer
from prometheus_client import Counter, Histogram, start_http_server

logger = logging.getLogger(__name__)

# ---- Metrics --------------------------------------------------------------
# These are the signals that answer "is the pipeline actually healthy":
# how much is flowing per source, how much is being dropped/failing, and
# how long it takes to process a message end to end.

MESSAGES_CONSUMED = Counter(
    "normalizer_messages_consumed_total",
    "Raw messages pulled off Kafka",
    ["source_id"],
)
MESSAGES_PROCESSED = Counter(
    "normalizer_messages_processed_total",
    "Messages successfully parsed and written to a normalized table",
    ["source_id", "table"],
)
MESSAGES_DROPPED = Counter(
    "normalizer_messages_dropped_total",
    "Messages dropped: no parser registered, or parser rejected the payload",
    ["source_id", "reason"],
)
MESSAGES_FAILED = Counter(
    "normalizer_messages_failed_total",
    "Messages that raised an unexpected exception during processing",
    ["source_id"],
)
PROCESSING_LATENCY = Histogram(
    "normalizer_processing_seconds",
    "Time to process a single message (parse + write to Postgres)",
    ["source_id"],
)


# ---- Source-specific parsers -----------------------------------------
# Each parser takes a raw payload dict and returns (table_name, row_dict)
# or None if the payload should be dropped (e.g. malformed, irrelevant).

def parse_binance_trade(payload: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    try:
        return "crypto_trades", {
            "symbol": payload["s"],
            "price": payload["p"],
            "quantity": payload["q"],
            "trade_time": datetime.fromtimestamp(payload["T"] / 1000, tz=timezone.utc),
            "is_buyer_maker": payload["m"],
        }
    except (KeyError, TypeError, ValueError) as e:
        logger.warning("Dropping malformed binance payload: %s (%s)", payload, e)
        return None


def parse_news_rss(payload: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    try:
        return "news_articles", {
            "article_id": payload["id"],
            "title": payload["title"],
            "summary": payload.get("summary", ""),
            "link": payload.get("link", ""),
            "author": payload.get("author", ""),
            "published_raw": payload.get("published", ""),
        }
    except (KeyError, TypeError) as e:
        logger.warning("Dropping malformed news payload: %s (%s)", payload, e)
        return None


PARSERS: dict[str, Callable] = {
    "binance": parse_binance_trade,
    "news_rss": parse_news_rss,
    # "pos_square": parse_pos_square,   <- next connector plugs in here
}


# ---- DB writes ----------------------------------------------------------

def insert_raw(cur, event: dict) -> None:
    cur.execute(
        """
        INSERT INTO raw_events (source_id, source_type, schema_version, ingested_at, payload)
        VALUES (%s, %s, %s, to_timestamp(%s), %s)
        """,
        (
            event["source_id"],
            event["source_type"],
            event["schema_version"],
            event["ingested_at"],
            json.dumps(event["payload"]),
        ),
    )


# Some tables need dedup behavior on insert (e.g. news articles can be
# re-seen after a connector restart). Map table -> ON CONFLICT clause;
# tables not listed here use a plain insert.
CONFLICT_CLAUSES: dict[str, str] = {
    "news_articles": "ON CONFLICT (article_id) DO NOTHING",
}


def insert_normalized(cur, table: str, row: dict, ingested_at: float) -> None:
    row = {**row, "ingested_at": datetime.fromtimestamp(ingested_at, tz=timezone.utc)}
    columns = ", ".join(row.keys())
    placeholders = ", ".join(["%s"] * len(row))
    conflict_clause = CONFLICT_CLAUSES.get(table, "")
    cur.execute(
        f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) {conflict_clause}",
        list(row.values()),
    )


# ---- Main loop ------------------------------------------------------------

async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    metrics_port = int(os.environ.get("METRICS_PORT", "9100"))
    start_http_server(metrics_port)
    logger.info("Metrics available on :%d/metrics", metrics_port)

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    conn.autocommit = True

    consumer = AIOKafkaConsumer(
        *os.environ["KAFKA_TOPICS_RAW"].split(","),
        bootstrap_servers=os.environ["KAFKA_BOOTSTRAP"],
        group_id=os.environ["KAFKA_GROUP_ID"],
        auto_offset_reset="earliest",
    )
    await consumer.start()
    logger.info("Normalizer consuming from topics: %s", os.environ["KAFKA_TOPICS_RAW"])

    try:
        async for msg in consumer:
            source_id = "unknown"
            start = time.perf_counter()
            try:
                event = json.loads(msg.value)
                source_id = event["source_id"]
                MESSAGES_CONSUMED.labels(source_id=source_id).inc()

                with conn.cursor() as cur:
                    insert_raw(cur, event)

                    parser = PARSERS.get(source_id)
                    if parser is None:
                        logger.warning("No parser registered for source '%s'", source_id)
                        MESSAGES_DROPPED.labels(source_id=source_id, reason="no_parser").inc()
                        continue

                    parsed = parser(event["payload"])
                    if parsed is None:
                        MESSAGES_DROPPED.labels(source_id=source_id, reason="parse_rejected").inc()
                        continue

                    table, row = parsed
                    insert_normalized(cur, table, row, event["ingested_at"])
                    MESSAGES_PROCESSED.labels(source_id=source_id, table=table).inc()
            except Exception:
                # One bad message shouldn't take down the whole consumer -
                # log it and keep processing the stream.
                MESSAGES_FAILED.labels(source_id=source_id).inc()
                logger.exception("Failed to process message, skipping")
            finally:
                PROCESSING_LATENCY.labels(source_id=source_id).observe(time.perf_counter() - start)
    finally:
        await consumer.stop()
        conn.close()


if __name__ == "__main__":
    asyncio.run(main())
