"""
Unit tests for the validation rules.
No Kafka or database needed: these run in milliseconds.
"""
import json

import pytest

from src.validation import classify_heart_rate, validate_message

VALID_TIMESTAMP = "2026-11-27T10:00:00+00:00"


def make_message(**overrides):
    """Build a valid JSON message, optionally changing some fields."""
    data = {"customer_id": "C001", "timestamp": VALID_TIMESTAMP, "heart_rate": 72}
    data.update(overrides)
    return json.dumps(data)


# ---------- Valid messages ----------

def test_valid_message_is_accepted():
    reading, reason = validate_message(make_message())

    assert reason is None
    assert reading["customer_id"] == "C001"
    assert reading["heart_rate"] == 72
    assert reading["status"] == "normal"
    assert reading["recorded_at"].tzinfo is not None


@pytest.mark.parametrize("heart_rate, expected_status", [
    (30, "low"),       # lowest valid value
    (59, "low"),
    (60, "normal"),    # boundary: normal starts at 60
    (100, "normal"),   # boundary: normal ends at 100
    (101, "high"),
    (220, "high"),     # highest valid value
])
def test_heart_rate_status(heart_rate, expected_status):
    reading, reason = validate_message(make_message(heart_rate=heart_rate))

    assert reason is None
    assert reading["status"] == expected_status


def test_classify_heart_rate():
    assert classify_heart_rate(45) == "low"
    assert classify_heart_rate(75) == "normal"
    assert classify_heart_rate(150) == "high"


# ---------- Invalid messages ----------

@pytest.mark.parametrize("raw_message, expected_reason", [
    ("not json at all", "invalid JSON"),
    ("[1, 2, 3]", "message is not a JSON object"),
    (json.dumps({"timestamp": VALID_TIMESTAMP, "heart_rate": 72}), "missing field: customer_id"),
    (json.dumps({"customer_id": "C001", "heart_rate": 72}), "missing field: timestamp"),
    (make_message(heart_rate=None), "missing field: heart_rate"),
    (make_message(customer_id=""), "invalid customer_id"),
    (make_message(customer_id=123), "invalid customer_id"),
    (make_message(timestamp="yesterday"), "invalid timestamp"),
    (make_message(timestamp="2026-11-27T10:00:00"), "timestamp has no timezone"),
    (make_message(heart_rate="72"), "heart_rate is not a whole number"),
    (make_message(heart_rate=72.5), "heart_rate is not a whole number"),
    (make_message(heart_rate=True), "heart_rate is not a whole number"),
    (make_message(heart_rate=-10), "heart_rate out of range: -10"),
    (make_message(heart_rate=0), "heart_rate out of range: 0"),
    (make_message(heart_rate=29), "heart_rate out of range: 29"),
    (make_message(heart_rate=221), "heart_rate out of range: 221"),
    (make_message(heart_rate=300), "heart_rate out of range: 300"),
])
def test_invalid_message_is_rejected(raw_message, expected_reason):
    reading, reason = validate_message(raw_message)

    assert reading is None
    assert reason == expected_reason