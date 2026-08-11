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
import re
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
            "feed_source": payload.get("feed_label", "default"),
        }
    except (KeyError, TypeError) as e:
        logger.warning("Dropping malformed news payload: %s (%s)", payload, e)
        return None


# ---- Retail CSV: this parser does actual data-quality work, not just
# key lookups. Real POS/accounting exports are inconsistent: prices show
# up as "$12.99", "12.99", or "12,99" depending on the till software;
# dates show up in whatever format the exporting system defaults to.
# The goal is to accept everything that's plausibly valid and only reject
# rows that are genuinely unusable.

_DATE_FORMATS = ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S", "%m-%d-%Y")


def _parse_date(raw: str):
    raw = (raw or "").strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _parse_money(raw: str):
    if raw is None:
        return None
    # Strip currency symbols, thousands separators, stray whitespace -
    # keep digits, a single decimal point, and a leading minus sign.
    cleaned = re.sub(r"[^\d.\-]", "", str(raw).replace(",", ""))
    if not cleaned or cleaned in ("-", "."):
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_retail_csv(payload: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    product = (payload.get("product_name") or payload.get("product") or "").strip()
    if not product:
        # A row with no product name isn't a usable transaction record -
        # this is the one thing we treat as a hard requirement.
        logger.warning(
            "Dropping retail row with no product name: file=%s row=%s",
            payload.get("_source_file"), payload.get("_row_number"),
        )
        return None

    unit_price = _parse_money(payload.get("unit_price") or payload.get("price"))
    quantity_raw = (payload.get("quantity") or "").strip()
    try:
        quantity = int(float(quantity_raw)) if quantity_raw else 1
    except ValueError:
        quantity = 1

    total = _parse_money(payload.get("total"))
    if total is None and unit_price is not None:
        total = round(unit_price * quantity, 2)

    trade_date = _parse_date(payload.get("transaction_date") or payload.get("date") or "")

    return "retail_transactions", {
        "product_name": product,
        "store_id": (payload.get("store_id") or "").strip() or None,
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": total,
        "transaction_date": trade_date,
        "payment_method": (payload.get("payment_method") or "").strip() or None,
        "source_file": payload.get("_source_file"),
        "source_row": payload.get("_row_number"),
        "org_slug": payload.get("_org_slug"),  # resolved to org_id in main() before insert
        "raw_row": json.dumps(payload),
    }


def parse_gl_import(payload: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    """
    Stages a line item from any financial system for GL reconciliation.
    Deliberately does NOT resolve the canonical account here - that lookup
    needs a DB connection and org context, which parsers don't have (they
    stay pure functions). Resolution happens in main() right before
    insert, same pattern as retail's org_slug -> org_id.
    """
    category = (payload.get("category") or payload.get("account") or "").strip()
    if not category:
        logger.warning(
            "Dropping GL row with no category: file=%s row=%s",
            payload.get("_source_file"), payload.get("_row_number"),
        )
        return None

    amount = _parse_money(payload.get("amount"))
    txn_date = _parse_date(payload.get("date") or payload.get("transaction_date") or "")

    return "gl_transactions", {
        "source_system": (payload.get("_source_system") or "unknown").strip().lower(),
        "source_category": category,
        "description": (payload.get("description") or payload.get("memo") or "").strip() or None,
        "amount": amount,
        "transaction_date": txn_date,
        "source_file": payload.get("_source_file"),
        "org_slug": payload.get("_org_slug"),  # resolved to org_id in main() before insert
        "raw_row": json.dumps(payload),
    }


PARSERS: dict[str, Callable] = {
    "binance": parse_binance_trade,
    "news_rss": parse_news_rss,
    "retail_csv": parse_retail_csv,
    "gl_import": parse_gl_import,
    # next connector plugs in here
}


# ---- Tenant resolution -----------------------------------------------
# retail_transactions rows arrive with an org_slug (from the connector's
# filename convention), which needs to become a real org_id before insert.
# Cached in memory since the same handful of orgs get looked up constantly
# and organizations essentially never change mid-run.

_org_slug_cache: dict[str, int | None] = {}


def resolve_org_id(cur, org_slug: str | None) -> int | None:
    if not org_slug:
        return None
    if org_slug in _org_slug_cache:
        return _org_slug_cache[org_slug]

    cur.execute("SELECT id FROM organizations WHERE slug = %s", (org_slug,))
    row = cur.fetchone()
    org_id = row[0] if row else None
    _org_slug_cache[org_slug] = org_id
    return org_id


# GL account resolution: (org_id, source_system, source_category) -> the
# canonical gl_account_id someone mapped it to, or None if nobody has
# mapped this (system, category) pair for this org yet. Unmapped is a
# normal, expected state - not an error - it just means this transaction
# needs a human to define the mapping once, after which every future
# transaction with that same category resolves automatically.
_gl_mapping_cache: dict[tuple[int, str, str], int | None] = {}


def resolve_gl_account(cur, org_id: int, source_system: str, source_category: str) -> int | None:
    key = (org_id, source_system, source_category)
    if key in _gl_mapping_cache:
        return _gl_mapping_cache[key]

    cur.execute(
        "SELECT gl_account_id FROM gl_mappings WHERE org_id = %s AND source_system = %s AND source_category = %s",
        (org_id, source_system, source_category),
    )
    row = cur.fetchone()
    gl_account_id = row[0] if row else None
    _gl_mapping_cache[key] = gl_account_id
    return gl_account_id


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

                    if table == "retail_transactions":
                        org_slug = row.pop("org_slug", None)
                        org_id = resolve_org_id(cur, org_slug)
                        if org_id is None:
                            logger.warning(
                                "Dropping retail row: unknown org slug '%s' (file=%s) - "
                                "has that business signed up yet?",
                                org_slug, row.get("source_file"),
                            )
                            MESSAGES_DROPPED.labels(source_id=source_id, reason="unknown_org").inc()
                            continue
                        row["org_id"] = org_id

                    if table == "gl_transactions":
                        org_slug = row.pop("org_slug", None)
                        org_id = resolve_org_id(cur, org_slug)
                        if org_id is None:
                            logger.warning(
                                "Dropping GL row: unknown org slug '%s' (file=%s)",
                                org_slug, row.get("source_file"),
                            )
                            MESSAGES_DROPPED.labels(source_id=source_id, reason="unknown_org").inc()
                            continue
                        row["org_id"] = org_id

                        # This is the actual reconciliation step: look up
                        # whether someone has already mapped this system's
                        # category to a canonical account. If not, the row
                        # still gets stored (never silently dropped) with
                        # canonical_gl_account_id left NULL - visible in
                        # the API/UI as "needs mapping" rather than lost.
                        gl_account_id = resolve_gl_account(cur, org_id, row["source_system"], row["source_category"])
                        row["canonical_gl_account_id"] = gl_account_id
                        if gl_account_id is None:
                            MESSAGES_DROPPED.labels(source_id=source_id, reason="unmapped_gl_category").inc()
                            # NOTE: this metric name is slightly misleading -
                            # the row is NOT dropped, it's stored unmapped.
                            # Kept under MESSAGES_DROPPED so it's visible in
                            # the same dashboard panel as other data-quality
                            # gaps worth a human's attention.

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
