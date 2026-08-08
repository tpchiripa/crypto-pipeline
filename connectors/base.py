"""
Connector interface.

Every data source (crypto, news, retail POS, weather, soccer scores, mining
sensors...) implements this same contract. The rest of the pipeline
(Kafka, normalizer, storage, API) never needs to know anything about the
source-specific details — it only ever sees a RawEvent envelope.

This is the piece that makes the platform "modular by design" rather than
one pipeline pretending to be general: adding a new source means writing
one small class here, not touching Kafka/normalizer/storage code.
"""
import abc
import json
import time
from dataclasses import dataclass, asdict
from typing import Any, AsyncIterator


@dataclass
class RawEvent:
    """
    Common envelope every connector must emit, regardless of source.
    The `payload` field carries the source's native, unmodified data —
    messiness is fine here. Cleaning happens downstream in the normalizer.
    """
    source_id: str          # e.g. "binance", "news_rss", "pos_square"
    source_type: str        # e.g. "crypto", "news", "retail", "weather"
    schema_version: str     # bump when a source's payload shape changes
    ingested_at: float      # unix timestamp, set at ingestion time
    payload: dict[str, Any] # raw, source-native data — unvalidated

    def to_json(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")

    def kafka_key(self) -> bytes:
        # Partition key — override per-source if you need ordering
        # guarantees on something more specific (e.g. per-symbol).
        return self.source_id.encode("utf-8")


class BaseConnector(abc.ABC):
    """
    Subclass this for every new data source.

    Required:
      - source_id / source_type: identify the source
      - stream(): an async generator yielding RawEvent objects forever

    The connector's ONLY job is: get data out of the source and into a
    RawEvent. No parsing/cleaning/validation belongs here — that's the
    normalizer's job, one layer downstream.
    """

    source_id: str
    source_type: str
    schema_version: str = "v1"

    @abc.abstractmethod
    async def stream(self) -> AsyncIterator[dict[str, Any]]:
        """Yield raw, source-native dicts as they arrive."""
        raise NotImplementedError

    async def events(self) -> AsyncIterator[RawEvent]:
        """Wraps stream() output into the common RawEvent envelope."""
        async for raw_payload in self.stream():
            yield RawEvent(
                source_id=self.source_id,
                source_type=self.source_type,
                schema_version=self.schema_version,
                ingested_at=time.time(),
                payload=raw_payload,
            )
