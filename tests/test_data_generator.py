"""
Unit tests for the synthetic data generator.
"""
from datetime import datetime

from src.data_generator import HeartRateGenerator


def test_batch_has_one_reading_per_customer():
    generator = HeartRateGenerator(num_customers=5, seed=1)
    batch = generator.generate_batch()

    assert len(batch) == 5
    assert {r["customer_id"] for r in batch} == {"C001", "C002", "C003", "C004", "C005"}


def test_reading_has_required_fields():
    generator = HeartRateGenerator(num_customers=1, seed=1)
    reading = generator.generate_reading("C001")

    assert set(reading) == {"customer_id", "timestamp", "heart_rate"}
    assert datetime.fromisoformat(reading["timestamp"]).tzinfo is not None  # UTC, with timezone


def test_same_seed_gives_same_heart_rates():
    first = HeartRateGenerator(seed=42)
    second = HeartRateGenerator(seed=42)

    for _ in range(20):
        rates_1 = [r["heart_rate"] for r in first.generate_batch()]
        rates_2 = [r["heart_rate"] for r in second.generate_batch()]
        assert rates_1 == rates_2


def test_without_anomalies_all_readings_stay_realistic():
    generator = HeartRateGenerator(anomaly_rate=0, invalid_rate=0, seed=7)

    for _ in range(500):
        for reading in generator.generate_batch():
            assert 55 <= reading["heart_rate"] <= 110


def test_heart_rate_changes_gradually():
    """Normal readings should drift a few beats, not jump randomly."""
    generator = HeartRateGenerator(num_customers=1, anomaly_rate=0, invalid_rate=0, seed=3)
    previous = generator.generate_reading("C001")["heart_rate"]

    for _ in range(200):
        current = generator.generate_reading("C001")["heart_rate"]
        assert abs(current - previous) <= 6
        previous = current


def test_invalid_rate_one_makes_every_reading_invalid():
    generator = HeartRateGenerator(anomaly_rate=0, invalid_rate=1, seed=5)

    for reading in generator.generate_batch():
        assert reading["heart_rate"] in (-10, 0, 300, None)