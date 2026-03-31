"""Kafka consumer: subscribes to policy.sales.raw and validates events."""

import json
import logging
import os
import sys

from confluent_kafka import Consumer, KafkaError
from pydantic import ValidationError

# Reuse the same schema the producer uses
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "producer"))
from schema import PolicySoldEvent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "policy.sales.raw")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "policy-consumer-group")


def create_consumer() -> Consumer:
    """Create and return a configured Kafka consumer."""
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    return Consumer(conf)


def validate_message(raw_value: bytes) -> PolicySoldEvent | None:
    """Deserialize and validate a Kafka message. Returns None if invalid."""
    try:
        data = json.loads(raw_value)
        event = PolicySoldEvent(**data)
        return event
    except (json.JSONDecodeError, ValidationError) as e:
        logger.error("Invalid message — skipping: %s", e)
        return None


def run():
    """Main loop: consume and validate messages from Kafka."""
    consumer = create_consumer()
    consumer.subscribe([TOPIC])
    logger.info("Consumer started — subscribed to '%s' (group: %s)", TOPIC, GROUP_ID)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("Reached end of partition %d", msg.partition())
                else:
                    logger.error("Consumer error: %s", msg.error())
                continue

            event = validate_message(msg.value())
            if event is None:
                continue

            logger.info(
                "Validated event %s — agent=%s product=%s premium=%d",
                event.event_id,
                event.agent_id,
                event.product_code,
                event.premium_amount,
            )

    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        consumer.close()
        logger.info("Consumer stopped.")


if __name__ == "__main__":
    run()