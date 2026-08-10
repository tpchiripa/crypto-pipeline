"""
News connector: polls a set of RSS feeds on an interval and emits new
articles. Supports multiple feeds so coverage can span world news,
business, and tech rather than one narrow query - each feed is tagged
with a label so the source stays visible downstream.

This is deliberately a very different shape of source from Binance:
- Binance pushes continuously over a WebSocket; RSS has to be polled.
- Binance never repeats a trade; an RSS feed returns the same articles
  on every poll until they age out, so this connector has to track what
  it's already seen and only emit genuinely new entries.
- The payload itself is unstructured text (title/summary), not numeric
  ticks.

None of that complexity leaks into the rest of the pipeline - it's fully
contained here, behind the same BaseConnector.stream() contract Binance
uses. That's the actual proof the architecture is modular: two connectors
that behave nothing alike, sharing one interface.
"""
import asyncio
import logging
import os
from typing import Any, AsyncIterator

import feedparser

from connectors.base import BaseConnector
from connectors.kafka_producer import run

logger = logging.getLogger(__name__)


class NewsRSSConnector(BaseConnector):
    source_id = "news_rss"
    source_type = "news"
    schema_version = "v1"

    def __init__(self, feeds: dict[str, str], poll_interval_seconds: int = 30):
        # feeds: {label: url} - e.g. {"world": "https://...", "tech": "https://..."}
        self.feeds = feeds
        self.poll_interval_seconds = poll_interval_seconds
        # Track article ids we've already emitted, so re-polling the same
        # feed doesn't republish the same articles forever. Shared across
        # all feeds - a URL collision across two feeds is vanishingly
        # unlikely and would just mean one fewer duplicate, not a bug.
        self._seen_ids: set[str] = set()

    def _entry_id(self, entry: dict) -> str:
        return entry.get("id") or entry.get("link", "")

    async def _poll_feed(self, label: str, url: str) -> AsyncIterator[dict[str, Any]]:
        logger.info("Polling feed [%s]: %s", label, url)
        parsed = await asyncio.to_thread(feedparser.parse, url)

        new_count = 0
        for entry in parsed.entries:
            entry_id = self._entry_id(entry)
            if not entry_id or entry_id in self._seen_ids:
                continue

            self._seen_ids.add(entry_id)
            new_count += 1
            yield {
                "id": entry_id,
                "feed_label": label,
                "title": entry.get("title", ""),
                "summary": entry.get("summary", ""),
                "link": entry.get("link", ""),
                "published": entry.get("published", ""),
                "author": entry.get("author", ""),
            }

        logger.info("Feed [%s]: found %d new article(s)", label, new_count)

    async def stream(self) -> AsyncIterator[dict[str, Any]]:
        while True:
            for label, url in self.feeds.items():
                try:
                    async for entry in self._poll_feed(label, url):
                        yield entry
                except Exception:
                    # One feed failing (bad URL, temporary outage) shouldn't
                    # take down polling for every other feed.
                    logger.exception("Feed [%s] failed to poll, skipping this cycle", label)

            if len(self._seen_ids) > 8000:
                self._seen_ids = set(list(self._seen_ids)[-4000:])

            await asyncio.sleep(self.poll_interval_seconds)


def _parse_feeds_env(raw: str) -> dict[str, str]:
    """
    Parses NEWS_FEEDS env var formatted as "label1=url1,label2=url2,...".
    Falls back to a single unlabeled feed if the format doesn't match
    (keeps backward compatibility with the old single-URL NEWS_FEED_URL).
    """
    feeds = {}
    for part in raw.split(","):
        if "=" in part:
            label, url = part.split("=", 1)
            feeds[label.strip()] = url.strip()
    return feeds


if __name__ == "__main__":
    feeds_raw = os.environ.get("NEWS_FEEDS")
    if feeds_raw:
        feeds = _parse_feeds_env(feeds_raw)
    else:
        # Backward-compatible single-feed fallback
        feeds = {"default": os.environ.get("NEWS_FEED_URL", "https://hnrss.org/newest?q=crypto")}

    poll_interval = int(os.environ.get("NEWS_POLL_INTERVAL", "30"))
    topic = os.environ.get("KAFKA_TOPIC_RAW", "raw.news.articles")
    connector = NewsRSSConnector(feeds=feeds, poll_interval_seconds=poll_interval)
    run(connector, topic)
