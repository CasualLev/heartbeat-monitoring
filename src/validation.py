"""
Validation rules for incoming heart rate messages.

These are plain functions with no Kafka or database code, so they are easy
to unit test.
"""
import json
from datetime import datetime

from src import config

REQUIRED_FIELDS = ("customer_id", "timestamp", "heart_rate")


def classify_heart_rate(heart_rate):
    """Return 'low', 'normal' or 'high' for a valid heart rate."""
    if heart_rate < config.LOW_HEART_RATE:
        return "low"
    if heart_rate > config.HIGH_HEART_RATE:
        return "high"
    return "normal"


def validate_message(raw_message):
    """
    Check one raw Kafka message.

    Returns (reading, None) if valid, or (None, reason) if invalid.
    A valid reading is a dict with customer_id, recorded_at, heart_rate, status.
    """
    # 1. Must be valid JSON describing an object
    try:
        data = json.loads(raw_message)
    except json.JSONDecodeError:
        return None, "invalid JSON"
    if not isinstance(data, dict):
        return None, "message is not a JSON object"

    # 2. All required fields must be present and not null
    for field in REQUIRED_FIELDS:
        if data.get(field) is None:
            return None, f"missing field: {field}"

    # 3. customer_id must be a non-empty string
    customer_id = data["customer_id"]
    if not isinstance(customer_id, str) or not customer_id.strip():
        return None, "invalid customer_id"

    # 4. timestamp must be ISO-8601 and include a timezone
    try:
        recorded_at = datetime.fromisoformat(data["timestamp"])
    except (TypeError, ValueError):
        return None, "invalid timestamp"
    if recorded_at.tzinfo is None:
        return None, "timestamp has no timezone"

    # 5. heart_rate must be a whole number in a physically possible range
    heart_rate = data["heart_rate"]
    if isinstance(heart_rate, bool) or not isinstance(heart_rate, int):
        return None, "heart_rate is not a whole number"
    if not config.MIN_VALID_HEART_RATE <= heart_rate <= config.MAX_VALID_HEART_RATE:
        return None, f"heart_rate out of range: {heart_rate}"

    reading = {
        "customer_id": customer_id,
        "recorded_at": recorded_at,
        "heart_rate": heart_rate,
        "status": classify_heart_rate(heart_rate),
    }
    return reading, None