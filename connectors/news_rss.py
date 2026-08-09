"""
News connector: polls an RSS feed on an interval and emits new articles.

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

    def __init__(self, feed_url: str, poll_interval_seconds: int = 30):
        self.feed_url = feed_url
        self.poll_interval_seconds = poll_interval_seconds
        # Track article ids we've already emitted, so re-polling the same
        # feed doesn't republish the same articles forever.
        self._seen_ids: set[str] = set()

    def _entry_id(self, entry: dict) -> str:
        # RSS entries usually have a stable 'id' (often the guid); fall back
        # to the link if a feed omits it.
        return entry.get("id") or entry.get("link", "")

    async def stream(self) -> AsyncIterator[dict[str, Any]]:
        while True:
            logger.info("Polling feed: %s", self.feed_url)
            # feedparser is synchronous/blocking - run it off the event loop
            # so it doesn't stall anything else.
            parsed = await asyncio.to_thread(feedparser.parse, self.feed_url)

            new_count = 0
            for entry in parsed.entries:
                entry_id = self._entry_id(entry)
                if not entry_id or entry_id in self._seen_ids:
                    continue

                self._seen_ids.add(entry_id)
                new_count += 1
                yield {
                    "id": entry_id,
                    "title": entry.get("title", ""),
                    "summary": entry.get("summary", ""),
                    "link": entry.get("link", ""),
                    "published": entry.get("published", ""),
                    "author": entry.get("author", ""),
                }

            # Cap memory growth - keep the seen-set from growing unbounded
            # across a long-running process.
            if len(self._seen_ids) > 5000:
                self._seen_ids = set(list(self._seen_ids)[-2500:])

            logger.info("Found %d new article(s)", new_count)
            await asyncio.sleep(self.poll_interval_seconds)


if __name__ == "__main__":
    feed_url = os.environ.get("NEWS_FEED_URL", "https://hnrss.org/newest?q=crypto")
    poll_interval = int(os.environ.get("NEWS_POLL_INTERVAL", "30"))
    topic = os.environ.get("KAFKA_TOPIC_RAW", "raw.news.articles")
    connector = NewsRSSConnector(feed_url=feed_url, poll_interval_seconds=poll_interval)
    run(connector, topic)
