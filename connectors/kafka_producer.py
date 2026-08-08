"""
Thin wrapper so connectors don't each reimplement Kafka setup/error handling.
"""
import asyncio
import logging
import os

from aiokafka import AIOKafkaProducer

from connectors.base import BaseConnector

logger = logging.getLogger(__name__)


async def run_connector_to_kafka(connector: BaseConnector, topic: str) -> None:
    bootstrap = os.environ["KAFKA_BOOTSTRAP"]
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)

    await producer.start()
    logger.info("Producer connected to %s, publishing to topic '%s'", bootstrap, topic)
    try:
        async for event in connector.events():
            await producer.send_and_wait(
                topic,
                value=event.to_json(),
                key=event.kafka_key(),
            )
            logger.debug("Published event from %s", event.source_id)
    finally:
        await producer.stop()


def run(connector: BaseConnector, topic: str) -> None:
    """Blocking entrypoint for a connector's __main__ script."""
    logging.basicConfig(level=logging.INFO)
    while True:
        try:
            asyncio.run(run_connector_to_kafka(connector, topic))
        except Exception:
            logger.exception("Connector crashed, restarting in 5s")
            import time
            time.sleep(5)
