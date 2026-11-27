"""
PostgreSQL helper functions: connect and save readings.
"""
import psycopg2

from src import config

INSERT_READING_SQL = """
    INSERT INTO heartbeat_readings (customer_id, recorded_at, heart_rate, status)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (customer_id, recorded_at) DO NOTHING
"""

INSERT_REJECTED_SQL = """
    INSERT INTO heartbeat_rejected (raw_message, reason)
    VALUES (%s, %s)
"""


def get_connection():
    """Open a connection to PostgreSQL using settings from .env."""
    return psycopg2.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        dbname=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
    )


def save_batch(conn, readings, rejected):
    """
    Save valid readings and rejected messages in ONE transaction.
    Either everything is saved, or nothing is (rollback on error).

    Returns the number of readings actually inserted (duplicates are skipped).
    """
    inserted = 0
    with conn:  # commits on success, rolls back on error
        with conn.cursor() as cur:
            for r in readings:
                cur.execute(
                    INSERT_READING_SQL,
                    (r["customer_id"], r["recorded_at"], r["heart_rate"], r["status"]),
                )
                inserted += cur.rowcount  # 1 = inserted, 0 = duplicate skipped

            for raw_message, reason in rejected:
                cur.execute(INSERT_REJECTED_SQL, (raw_message, reason))
    return inserted