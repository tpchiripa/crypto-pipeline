"""
Hospitality connector: watches a folder for CSV ingredient/inventory
exports and stages them for ingestion. Same file-drop pattern as
retail_csv.py - the genuinely new piece isn't the ingestion mechanism,
it's what the normalizer does with the data once it arrives: hospitality
inventory is notoriously inconsistent about units (one supplier invoices
in kg, another in lbs, a third in "each" for produce), and that
inconsistency is exactly the kind of real-world messiness this platform
is built to absorb rather than choke on.
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


class HospitalityCSVConnector(BaseConnector):
    source_id = "hospitality_csv"
    source_type = "hospitality"
    schema_version = "v1"

    def __init__(self, watch_dir: str, processed_dir: str, poll_interval_seconds: int = 5):
        self.watch_dir = Path(watch_dir)
        self.processed_dir = Path(processed_dir)
        self.poll_interval_seconds = poll_interval_seconds
        self.watch_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)

    def _is_file_stable(self, path: Path) -> bool:
        try:
            return path.stat().st_size > 0
        except FileNotFoundError:
            return False

    def _read_csv_rows(self, path: Path) -> list[dict[str, Any]]:
        # Filename convention: <org-slug>_<anything>.csv, same as retail.
        org_slug = path.stem.split("_", 1)[0] if "_" in path.stem else None

        rows = []
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                rows.append({
                    "_source_file": path.name,
                    "_row_number": i + 1,
                    "_org_slug": org_slug,
                    **row,
                })
        return rows

    async def stream(self) -> AsyncIterator[dict[str, Any]]:
        while True:
            for path in sorted(self.watch_dir.glob("*.csv")):
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
    watch_dir = os.environ.get("HOSPITALITY_WATCH_DIR", "/app/data/hospitality_incoming")
    processed_dir = os.environ.get("HOSPITALITY_PROCESSED_DIR", "/app/data/hospitality_processed")
    poll_interval = int(os.environ.get("HOSPITALITY_POLL_INTERVAL", "5"))
    topic = os.environ.get("KAFKA_TOPIC_RAW", "raw.hospitality.inventory")

    connector = HospitalityCSVConnector(
        watch_dir=watch_dir,
        processed_dir=processed_dir,
        poll_interval_seconds=poll_interval,
    )
    run(connector, topic)
