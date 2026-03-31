"""Kafka producer: generates and publishes policy_sold events."""

import logging
import os
import time

from confluent_kafka import Producer

from generator import generate_event

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "policy.sales.raw")
PUBLISH_INTERVAL = float(os.getenv("PUBLISH_INTERVAL_SEC", "2"))


def delivery_callback(err, msg):
    """Called once per message to indicate delivery result."""
    if err is not None:
        logger.error("Delivery failed for %s: %s", msg.key(), err)
    else:
        logger.info(
            "Published event to %s [partition %d] @ offset %d",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


def create_producer() -> Producer:
    """Create and return a configured Kafka producer."""
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "policy-producer",
        "acks": "all",
    }
    return Producer(conf)


def run():
    """Main loop: generate events and publish to Kafka."""
    producer = create_producer()
    logger.info("Producer started — publishing to '%s' every %.1fs", TOPIC, PUBLISH_INTERVAL)

    try:
        while True:
            event = generate_event()
            producer.produce(
                topic=TOPIC,
                key=event.agent_id,
                value=event.model_dump_json(),
                callback=delivery_callback,
            )
            # Trigger delivery callbacks for previously produced messages
            producer.poll(0)
            time.sleep(PUBLISH_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
    finally:
        # Wait for all messages to be delivered
        producer.flush(timeout=10)
        logger.info("Producer stopped.")


if __name__ == "__main__":
    run()