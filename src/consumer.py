"""
Kafka consumer.

Reads heart rate messages from Kafka, validates them, flags anomalies,
and stores the results in PostgreSQL:
  - valid readings  -> heartbeat_readings
  - invalid messages -> heartbeat_rejected (dead-letter table)

Offsets are committed to Kafka only AFTER the database write succeeds,
so no message is lost if the consumer crashes (at-least-once delivery).
"""
import logging

from confluent_kafka import Consumer

from src import config
from src.database import get_connection, save_batch
from src.validation import validate_message

logger = logging.getLogger(__name__)


def create_consumer():
    """Create a Kafka consumer that joins the consumer group and subscribes to the topic."""
    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": config.KAFKA_CONSUMER_GROUP,
        "auto.offset.reset": "earliest",   # first run: start from the oldest message
        "enable.auto.commit": False,       # we commit ourselves, after saving to the DB
    })
    consumer.subscribe([config.KAFKA_TOPIC])
    return consumer


def process_messages(messages):
    """Split a batch of Kafka messages into valid readings and rejected messages."""
    readings, rejected = [], []

    for msg in messages:
        if msg.error():
            logger.error("Kafka error: %s", msg.error())
            continue

        raw_message = msg.value().decode("utf-8", errors="replace")
        reading, reason = validate_message(raw_message)

        if reading is None:
            logger.warning("Rejected message (%s): %s", reason, raw_message)
            rejected.append((raw_message, reason))
            continue

        if reading["status"] != "normal":
            logger.warning(
                "ALERT: customer %s heart rate %d bpm (%s)",
                reading["customer_id"], reading["heart_rate"], reading["status"],
            )
        readings.append(reading)

    return readings, rejected


def main():
    config.setup_logging()
    consumer = create_consumer()
    conn = get_connection()
    logger.info(
        "Consumer started: topic '%s', group '%s'. Press Ctrl+C to stop.",
        config.KAFKA_TOPIC, config.KAFKA_CONSUMER_GROUP,
    )

    totals = {"stored": 0, "rejected": 0, "duplicates": 0}
    try:
        while True:
            # Read up to 100 messages, waiting at most 1 second
            messages = consumer.consume(num_messages=100, timeout=1.0)
            if not messages:
                continue

            readings, rejected = process_messages(messages)
            inserted = save_batch(conn, readings, rejected)

            # Only now tell Kafka these messages are done
            consumer.commit(asynchronous=False)

            totals["stored"] += inserted
            totals["rejected"] += len(rejected)
            totals["duplicates"] += len(readings) - inserted
            logger.info(
                "Batch of %d: stored %d, rejected %d, duplicates %d | totals: %s",
                len(messages), inserted, len(rejected), len(readings) - inserted, totals,
            )
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    except Exception:
        # e.g. database down: offsets were NOT committed, so restarting
        # the consumer will re-read and retry the same messages.
        logger.exception("Consumer stopped because of an error")
        raise
    finally:
        consumer.close()  # leave the consumer group cleanly
        conn.close()
        logger.info("Consumer stopped. Totals: %s", totals)


if __name__ == "__main__":
    main()