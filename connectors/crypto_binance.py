"""
Crypto connector: streams live trade ticks from Binance's public WebSocket API.

No API key required. This is true push-based streaming (not polling) -
Binance pushes a message every time a trade executes on a symbol.

Docs: https://binance-docs.github.io/apidocs/spot/en/#trade-streams
"""
import json
import logging
import os
from typing import Any, AsyncIterator

import websockets

from connectors.base import BaseConnector
from connectors.kafka_producer import run

logger = logging.getLogger(__name__)

BINANCE_WS_BASE = "wss://stream.binance.com:9443/stream"


class BinanceTradeConnector(BaseConnector):
    source_id = "binance"
    source_type = "crypto"
    schema_version = "v1"

    def __init__(self, symbols: list[str]):
        # Binance stream names are lowercase, e.g. "btcusdt@trade"
        self.symbols = [s.lower() for s in symbols]
        streams = "/".join(f"{s}@trade" for s in self.symbols)
        self.url = f"{BINANCE_WS_BASE}?streams={streams}"

    async def stream(self) -> AsyncIterator[dict[str, Any]]:
        logger.info("Connecting to Binance: %s", self.url)
        async for ws in websockets.connect(self.url, ping_interval=20):
            try:
                async for message in ws:
                    envelope = json.loads(message)
                    # Combined-stream format wraps the actual trade in "data"
                    data = envelope.get("data", envelope)
                    yield data
            except websockets.ConnectionClosed:
                logger.warning("Binance WS connection closed, reconnecting...")
                continue


if __name__ == "__main__":
    symbols = os.environ.get("BINANCE_SYMBOLS", "btcusdt").split(",")
    topic = os.environ.get("KAFKA_TOPIC_RAW", "raw.crypto.trades")
    connector = BinanceTradeConnector(symbols=symbols)
    run(connector, topic)
