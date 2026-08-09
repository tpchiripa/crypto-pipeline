"""
Retail connector: watches a folder for CSV files and ingests them as batch
transactions. This is a deliberately different ingestion pattern from the
other two connectors:

- Binance: continuous push over a WebSocket
- News RSS: polling an API, deduping by article id in memory
- Retail (this one): batch files landing in a folder, deduped by MOVING
  the file to a processed/ folder once ingested - the "have I seen this"
  state lives on disk, not in memory, which is how most real-world batch
  ingestion actually works (a hotel PMS, POS system, or accounting export
  drops a file; something picks it up and moves/archives it).

Real files are messy: missing fields, inconsistent formatting, occasional
garbage rows. This connector is deliberately permissive at the CSV-reading
level - it yields every row, malformed or not, and lets the normalizer's
parser decide what to do with each one. That mirrors how the rest of the
pipeline already works: connectors move data, parsers judge it.
"""
import asyncio
import csv
import logging
import os
import shutil
from pathlib import Path
from typing import Any, AsyncIterator

from connectors.base import BaseConnector
from connectors.kafka_producer import run

logger = logging.getLogger(__name__)


class RetailCSVConnector(BaseConnector):
    source_id = "retail_csv"
    source_type = "retail"
    schema_version = "v1"

    def __init__(self, watch_dir: str, processed_dir: str, poll_interval_seconds: int = 5):
        self.watch_dir = Path(watch_dir)
        self.processed_dir = Path(processed_dir)
        self.poll_interval_seconds = poll_interval_seconds
        self.watch_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)

    def _is_file_stable(self, path: Path) -> bool:
        try:
            size_a = path.stat().st_size
        except FileNotFoundError:
            return False
        return size_a > 0

    def _read_csv_rows(self, path: Path) -> list[dict[str, Any]]:
        rows = []
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                rows.append({
                    "_source_file": path.name,
                    "_row_number": i + 1,
                    **row,
                })
        return rows

    async def stream(self) -> AsyncIterator[dict[str, Any]]:
        while True:
            csv_files = sorted(self.watch_dir.glob("*.csv"))

            for path in csv_files:
                if not self._is_file_stable(path):
                    continue

                logger.info("Found new file: %s", path.name)
                try:
                    rows = self._read_csv_rows(path)
                except Exception:
                    logger.exception("Failed to read %s, leaving in place for inspection", path.name)
                    continue

                for row in rows:
                    yield row

                dest = self.processed_dir / path.name
                shutil.move(str(path), str(dest))
                logger.info("Ingested %d row(s) from %s, moved to processed/", len(rows), path.name)

            await asyncio.sleep(self.poll_interval_seconds)


if __name__ == "__main__":
    watch_dir = os.environ.get("RETAIL_WATCH_DIR", "/app/data/incoming")
    processed_dir = os.environ.get("RETAIL_PROCESSED_DIR", "/app/data/processed")
    poll_interval = int(os.environ.get("RETAIL_POLL_INTERVAL", "5"))
    topic = os.environ.get("KAFKA_TOPIC_RAW", "raw.retail.transactions")

    connector = RetailCSVConnector(
        watch_dir=watch_dir,
        processed_dir=processed_dir,
        poll_interval_seconds=poll_interval,
    )
    run(connector, topic)
