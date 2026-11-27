"""
End-to-end test of the whole pipeline:
    test messages -> Kafka -> consumer -> PostgreSQL

Requirements:
  - docker compose is up (Kafka + PostgreSQL)
  - the consumer is running:  python -m src.consumer

Run with:  pytest -m integration -v
"""
import json
import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from confluent_kafka import Producer

from src import config
from src.database import get_connection

pytestmark = pytest.mark.integration

WAIT_SECONDS = 30  # how long to wait for the consumer to process the messages


# ---------- Helpers ----------

@pytest.fixture
def test_id():
    """A unique ID for this test run, so we only look at our own test data."""
    return "T" + uuid.uuid4().hex[:8]


@pytest.fixture
def db():
    conn = get_connection()
    conn.autocommit = True
    yield conn
    conn.close()


@pytest.fixture(autouse=True)
def cleanup(db, test_id):
    """Delete this run's test data after the test, even if it fails."""
    yield
    with db.cursor() as cur:
        cur.execute("DELETE FROM heartbeat_readings WHERE customer_id LIKE %s", (test_id + "%",))
        cur.execute("DELETE FROM heartbeat_rejected WHERE raw_message LIKE %s", ("%" + test_id + "%",))


def send_to_kafka(raw_messages, key):
    """Send test messages. One key = one partition, so they keep their order."""
    producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
    for raw in raw_messages:
        producer.produce(config.KAFKA_TOPIC, key=key, value=raw)
    remaining = producer.flush(10)
    assert remaining == 0, "Could not deliver test messages to Kafka. Is it running?"


def wait_for_count(db, sql, params, expected):
    """Run a COUNT query until it returns the expected number, or time out."""
    count = None
    deadline = time.time() + WAIT_SECONDS
    while time.time() < deadline:
        with db.cursor() as cur:
            cur.execute(sql, params)
            count = cur.fetchone()[0]
        if count >= expected:
            return count
        time.sleep(1)
    pytest.fail(f"Expected {expected} rows, found {count} after {WAIT_SECONDS}s. Is the consumer running?")


def reading(customer_id, heart_rate, seconds_ago=0):
    timestamp = datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)
    return json.dumps({
        "customer_id": customer_id,
        "timestamp": timestamp.isoformat(),
        "heart_rate": heart_rate,
    })


# ---------- Tests ----------

def test_valid_and_anomalous_readings_are_stored_with_status(db, test_id):
    send_to_kafka([
        reading(f"{test_id}-N", 72),   # normal
        reading(f"{test_id}-L", 45),   # low anomaly
        reading(f"{test_id}-H", 150),  # high anomaly
    ], key=test_id)

    wait_for_count(db, "SELECT COUNT(*) FROM heartbeat_readings WHERE customer_id LIKE %s",
                   (test_id + "%",), expected=3)

    with db.cursor() as cur:
        cur.execute(
            "SELECT customer_id, status FROM heartbeat_readings WHERE customer_id LIKE %s",
            (test_id + "%",),
        )
        statuses = dict(cur.fetchall())

    assert statuses == {
        f"{test_id}-N": "normal",
        f"{test_id}-L": "low",
        f"{test_id}-H": "high",
    }


def test_invalid_messages_go_to_rejected_table(db, test_id):
    send_to_kafka([
        reading(test_id, 300),                    # out of range
        reading(test_id, None),                   # missing heart rate
        f"this is not json {test_id}",            # broken message
    ], key=test_id)

    wait_for_count(db, "SELECT COUNT(*) FROM heartbeat_rejected WHERE raw_message LIKE %s",
                   ("%" + test_id + "%",), expected=3)

    with db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM heartbeat_readings WHERE customer_id = %s", (test_id,))
        assert cur.fetchone()[0] == 0  # nothing invalid reached the main table


def test_duplicate_messages_are_stored_once(db, test_id):
    message = reading(test_id, 80)
    marker = reading(f"{test_id}-END", 80)  # sent last, tells us the consumer has caught up

    send_to_kafka([message, message, message, marker], key=test_id)

    wait_for_count(db, "SELECT COUNT(*) FROM heartbeat_readings WHERE customer_id = %s",
                   (f"{test_id}-END",), expected=1)

    with db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM heartbeat_readings WHERE customer_id = %s", (test_id,))
        assert cur.fetchone()[0] == 1