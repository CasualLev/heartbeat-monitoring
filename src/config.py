"""
Central configuration for the pipeline.

All settings are read from environment variables (loaded from the .env file),
so no passwords or machine-specific values are hard-coded in the scripts.
"""
import logging
import os

from dotenv import load_dotenv

load_dotenv()

# ---------- Kafka ----------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "heartbeat-readings")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "heartbeat-consumer-group")

# ---------- PostgreSQL ----------
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "heartbeat_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "heartbeat_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")  # no default for secrets

# ---------- Simulation ----------
NUM_CUSTOMERS = int(os.getenv("NUM_CUSTOMERS", "10"))
SEND_INTERVAL_SECONDS = float(os.getenv("SEND_INTERVAL_SECONDS", "1.0"))
ANOMALY_RATE = float(os.getenv("ANOMALY_RATE", "0.05"))  # 5% abnormal but possible
INVALID_RATE = float(os.getenv("INVALID_RATE", "0.02"))  # 2% broken readings

# ---------- Validation thresholds (beats per minute) ----------
MIN_VALID_HEART_RATE = 30    # below this is physically implausible -> rejected
MAX_VALID_HEART_RATE = 220   # above this is physically implausible -> rejected
LOW_HEART_RATE = 60          # below this -> status "low"
HIGH_HEART_RATE = 100        # above this -> status "high"

# ---------- Logging ----------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")


def setup_logging():
    """Use the same log format in every script."""
    logging.basicConfig(
        level=LOG_LEVEL,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )