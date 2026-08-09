"""
Thin wrapper so connectors don't each reimplement Kafka setup/error handling.
"""
import asyncio
import logging
import os
import time

from aiokafka import AIOKafkaProducer

from connectors.base import BaseConnector
from connectors.metrics import EVENTS_PUBLISHED, PUBLISH_ERRORS, LAST_EVENT_TIMESTAMP, start_metrics_server

logger = logging.getLogger(__name__)


async def run_connector_to_kafka(connector: BaseConnector, topic: str) -> None:
    bootstrap = os.environ["KAFKA_BOOTSTRAP"]
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)

    await producer.start()
    logger.info("Producer connected to %s, publishing to topic '%s'", bootstrap, topic)
    try:
        async for event in connector.events():
            try:
                await producer.send_and_wait(
                    topic,
                    value=event.to_json(),
                    key=event.kafka_key(),
                )
                EVENTS_PUBLISHED.labels(source_id=event.source_id).inc()
                LAST_EVENT_TIMESTAMP.labels(source_id=event.source_id).set(time.time())
                logger.debug("Published event from %s", event.source_id)
            except Exception:
                PUBLISH_ERRORS.labels(source_id=event.source_id).inc()
                raise
    finally:
        await producer.stop()


def run(connector: BaseConnector, topic: str) -> None:
    """Blocking entrypoint for a connector's __main__ script."""
    logging.basicConfig(level=logging.INFO)

    metrics_port = int(os.environ.get("METRICS_PORT", "9100"))
    start_metrics_server(metrics_port)
    logger.info("Metrics available on :%d/metrics", metrics_port)

    while True:
        try:
            asyncio.run(run_connector_to_kafka(connector, topic))
        except Exception:
            PUBLISH_ERRORS.labels(source_id=connector.source_id).inc()
            logger.exception("Connector crashed, restarting in 5s")
            time.sleep(5)
