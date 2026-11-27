"""
Kafka producer.

Takes readings from the HeartRateGenerator and sends them continuously to
the Kafka topic, simulating a real-time feed from heart rate monitors.
"""
import json
import logging
import time

from confluent_kafka import Producer

from src import config
from src.data_generator import HeartRateGenerator

logger = logging.getLogger(__name__)

# Counts updated by delivery_report (Kafka confirms messages in the background)
stats = {"delivered": 0, "failed": 0}


def create_producer():
    """Create a Kafka producer with safe delivery settings."""
    return Producer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "heartbeat-producer",
        "acks": "all",                 # wait until Kafka has safely stored the message
        "enable.idempotence": True,    # retries never create duplicate messages in Kafka
        "linger.ms": 10,               # wait up to 10 ms to send messages in small batches
    })


def delivery_report(err, msg):
    """Called once per message to tell us if Kafka received it."""
    if err is not None:
        stats["failed"] += 1
        logger.error("Delivery failed for customer %s: %s", msg.key().decode(), err)
    else:
        stats["delivered"] += 1
        logger.debug(
            "Delivered customer %s -> partition %d, offset %d",
            msg.key().decode(), msg.partition(), msg.offset(),
        )


def send_reading(producer, reading):
    """Send one reading. The customer_id is the key, so each customer stays in order."""
    key = reading["customer_id"]
    value = json.dumps(reading)
    try:
        producer.produce(config.KAFKA_TOPIC, key=key, value=value, on_delivery=delivery_report)
    except BufferError:
        # Local send queue is full: wait for Kafka to catch up, then try once more
        logger.warning("Producer queue full, waiting...")
        producer.poll(1)
        producer.produce(config.KAFKA_TOPIC, key=key, value=value, on_delivery=delivery_report)


def main():
    config.setup_logging()
    producer = create_producer()
    generator = HeartRateGenerator()

    logger.info(
        "Producer started: %d customers -> topic '%s' every %.1fs. Press Ctrl+C to stop.",
        len(generator.baselines), config.KAFKA_TOPIC, config.SEND_INTERVAL_SECONDS,
    )

    total_queued = 0
    try:
        while True:
            batch = generator.generate_batch()
            for reading in batch:
                send_reading(producer, reading)

            producer.poll(0)  # triggers delivery_report for messages already confirmed
            total_queued += len(batch)
            logger.info(
                "Queued %d readings | delivered: %d | failed: %d",
                len(batch), stats["delivered"], stats["failed"],
            )

            time.sleep(config.SEND_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
    finally:
        # Make sure messages still waiting in memory reach Kafka before exiting
        not_delivered = producer.flush(10)
        if not_delivered:
            logger.warning("%d messages were not delivered", not_delivered)
        logger.info(
            "Producer stopped. Queued: %d | delivered: %d | failed: %d",
            total_queued, stats["delivered"], stats["failed"],
        )


if __name__ == "__main__":
    main()