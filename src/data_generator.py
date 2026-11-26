"""
Synthetic heart rate data generator.

Simulates heart rate monitors for several customers. Each customer has a
personal resting heart rate (baseline), and each new reading drifts a little
from the previous one instead of jumping randomly, like a real sensor.

A small share of readings are deliberately abnormal (anomalies) or broken
(invalid) so the consumer's validation logic gets exercised.
"""
import logging
import random
import time
from datetime import datetime, timezone

from src import config

logger = logging.getLogger(__name__)


class HeartRateGenerator:

    def __init__(
        self,
        num_customers=config.NUM_CUSTOMERS,
        anomaly_rate=config.ANOMALY_RATE,
        invalid_rate=config.INVALID_RATE,
        seed=None,
    ):
        # Own random generator: passing a seed makes the output repeatable (useful in tests)
        self.rng = random.Random(seed)
        self.anomaly_rate = anomaly_rate
        self.invalid_rate = invalid_rate

        # Customer IDs C001, C002, ... each with a personal resting heart rate
        self.baselines = {
            f"C{i:03d}": self.rng.randint(62, 85) for i in range(1, num_customers + 1)
        }
        # Every customer starts at their baseline
        self.current = dict(self.baselines)

    def _next_normal_rate(self, customer_id):
        """Move the heart rate a little, with a gentle pull back toward the baseline."""
        baseline = self.baselines[customer_id]
        current = self.current[customer_id]

        drift = self.rng.randint(-3, 3)          # small random change
        pull = round((baseline - current) * 0.1)  # stops it wandering too far
        new_rate = max(55, min(110, current + drift + pull))

        self.current[customer_id] = new_rate
        return new_rate

    def generate_reading(self, customer_id):
        """Create one reading for one customer."""
        heart_rate = self._next_normal_rate(customer_id)

        roll = self.rng.random()
        if roll < self.invalid_rate:
            # Broken sensor data -> should be rejected by the consumer
            heart_rate = self.rng.choice([-10, 0, 300, None])
        elif roll < self.invalid_rate + self.anomaly_rate:
            # Abnormal but possible -> should be stored and flagged
            heart_rate = self.rng.choice([self.rng.randint(35, 55), self.rng.randint(130, 180)])

        return {
            "customer_id": customer_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "heart_rate": heart_rate,
        }

    def generate_batch(self):
        """One reading per customer, like every device reporting once per tick."""
        return [self.generate_reading(customer_id) for customer_id in self.baselines]


if __name__ == "__main__":
    # Preview the data without Kafka: python -m src.data_generator
    config.setup_logging()
    generator = HeartRateGenerator()
    logger.info("Generating data for %d customers. Press Ctrl+C to stop.", len(generator.baselines))

    try:
        while True:
            for reading in generator.generate_batch():
                logger.info(reading)
            time.sleep(config.SEND_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info("Stopped.") 