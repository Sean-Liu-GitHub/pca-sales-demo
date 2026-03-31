"""Kafka consumer: subscribes to policy.sales.raw, validates, and writes to PostgreSQL."""

import json
import logging
import os
import sys

import psycopg2
from confluent_kafka import Consumer, KafkaError
from pydantic import ValidationError

# Reuse the same schema the producer uses
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "producer"))
from schema import PolicySoldEvent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "policy.sales.raw")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "policy-consumer-group")

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "pca_sales")
PG_USER = os.getenv("POSTGRES_USER", "pca")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pca_secret")

INSERT_SQL = """
    INSERT INTO raw.policy_sales (
        event_id, event_type, event_ts, policy_id, agent_id,
        product_code, product_type, region, channel,
        premium_amount, sum_assured, customer_age_band, payment_frequency
    ) VALUES (
        %(event_id)s, %(event_type)s, %(event_ts)s, %(policy_id)s, %(agent_id)s,
        %(product_code)s, %(product_type)s, %(region)s, %(channel)s,
        %(premium_amount)s, %(sum_assured)s, %(customer_age_band)s, %(payment_frequency)s
    )
    ON CONFLICT (event_id) DO NOTHING;
"""


def create_consumer() -> Consumer:
    """Create and return a configured Kafka consumer."""
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    return Consumer(conf)


def connect_postgres():
    """Create and return a PostgreSQL connection."""
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )
    conn.autocommit = True
    return conn


def validate_message(raw_value: bytes) -> PolicySoldEvent | None:
    """Deserialize and validate a Kafka message. Returns None if invalid."""
    try:
        data = json.loads(raw_value)
        event = PolicySoldEvent(**data)
        return event
    except (json.JSONDecodeError, ValidationError) as e:
        logger.error("Invalid message — skipping: %s", e)
        return None


def write_to_postgres(conn, event: PolicySoldEvent):
    """Insert a validated event into raw.policy_sales."""
    try:
        with conn.cursor() as cur:
            cur.execute(INSERT_SQL, {
                "event_id": str(event.event_id),
                "event_type": event.event_type,
                "event_ts": event.event_ts.isoformat(),
                "policy_id": str(event.policy_id),
                "agent_id": event.agent_id,
                "product_code": event.product_code,
                "product_type": event.product_type,
                "region": event.region,
                "channel": event.channel,
                "premium_amount": event.premium_amount,
                "sum_assured": event.sum_assured,
                "customer_age_band": event.customer_age_band,
                "payment_frequency": event.payment_frequency,
            })
    except psycopg2.Error as e:
        logger.error("Failed to write event %s to PostgreSQL: %s", event.event_id, e)


def run():
    """Main loop: consume, validate, and write messages to PostgreSQL."""
    consumer = create_consumer()
    consumer.subscribe([TOPIC])
    conn = connect_postgres()
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

            write_to_postgres(conn, event)

            logger.info(
                "Ingested event %s — agent=%s product=%s premium=%d",
                event.event_id,
                event.agent_id,
                event.product_code,
                event.premium_amount,
            )

    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        consumer.close()
        conn.close()
        logger.info("Consumer stopped.")


if __name__ == "__main__":
    run()