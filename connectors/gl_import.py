"""
GL import connector: watches a folder for CSV exports from ANY financial
system (Dyner, Lightspeed, Xero, or anything else that can export a CSV)
and stages them for GL reconciliation.

Same file-drop pattern as retail_csv.py, but the filename encodes two
things instead of one:

    <org-slug>__<source-system>_<anything>.csv

e.g. acme-retail__dyner_purchases_aug10.csv
     acme-retail__lightspeed_sales_aug10.csv
     acme-retail__xero_gl_export_aug10.csv

This connector doesn't know or care what "Dyner" or "Xero" even are -
it just reads whichever CSV shows up and tags each row with which system
and which org it came from. The actual account-mapping intelligence
lives downstream in the normalizer + gl_mappings table, not here. That
separation is deliberate: this connector could ingest a fourth or fifth
system tomorrow with zero code changes.
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


class GLImportConnector(BaseConnector):
    source_id = "gl_import"
    source_type = "finance"
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

    def _parse_filename(self, path: Path) -> tuple[str | None, str | None]:
        # "<org-slug>__<source-system>_rest" -> (org_slug, source_system)
        stem = path.stem
        if "__" not in stem:
            return None, None
        org_slug, remainder = stem.split("__", 1)
        source_system = remainder.split("_", 1)[0] if "_" in remainder else remainder
        return org_slug, source_system

    def _read_csv_rows(self, path: Path) -> list[dict[str, Any]]:
        org_slug, source_system = self._parse_filename(path)
        rows = []
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                rows.append({
                    "_source_file": path.name,
                    "_row_number": i + 1,
                    "_org_slug": org_slug,
                    "_source_system": source_system,
                    **row,
                })
        return rows

    async def stream(self) -> AsyncIterator[dict[str, Any]]:
        while True:
            for path in sorted(self.watch_dir.glob("*.csv")):
                if not self._is_file_stable(path):
                    continue

                org_slug, source_system = self._parse_filename(path)
                if not org_slug or not source_system:
                    logger.warning(
                        "Skipping %s - filename doesn't match <org-slug>__<system>_....csv",
                        path.name,
                    )
                    continue

                logger.info("Found new GL file: %s (org=%s, system=%s)", path.name, org_slug, source_system)
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
    watch_dir = os.environ.get("GL_WATCH_DIR", "/app/data/gl_incoming")
    processed_dir = os.environ.get("GL_PROCESSED_DIR", "/app/data/gl_processed")
    poll_interval = int(os.environ.get("GL_POLL_INTERVAL", "5"))
    topic = os.environ.get("KAFKA_TOPIC_RAW", "raw.gl.transactions")

    connector = GLImportConnector(
        watch_dir=watch_dir,
        processed_dir=processed_dir,
        poll_interval_seconds=poll_interval,
    )
    run(connector, topic)
