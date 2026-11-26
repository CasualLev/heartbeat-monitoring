-- =====================================================================
-- Real-Time Customer Heart Beat Monitoring - Database Schema
-- =====================================================================

-- Valid heart rate readings (including flagged anomalies)
CREATE TABLE IF NOT EXISTS heartbeat_readings (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id  VARCHAR(20)  NOT NULL,
    recorded_at  TIMESTAMPTZ  NOT NULL,                 -- event time (from sensor)
    heart_rate   SMALLINT     NOT NULL,                 -- beats per minute
    status       VARCHAR(10)  NOT NULL,                 -- low / normal / high
    ingested_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),   -- processing time

    CONSTRAINT chk_heart_rate_range CHECK (heart_rate BETWEEN 30 AND 220),
    CONSTRAINT chk_status           CHECK (status IN ('low', 'normal', 'high')),
    CONSTRAINT uq_customer_reading  UNIQUE (customer_id, recorded_at)
);

-- Fast time-range queries, e.g. "readings from the last 10 minutes"
CREATE INDEX IF NOT EXISTS idx_readings_recorded_at
    ON heartbeat_readings (recorded_at);

-- Dead-letter table: messages that failed validation
CREATE TABLE IF NOT EXISTS heartbeat_rejected (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    raw_message  TEXT         NOT NULL,                 -- stored as-is, even if not valid JSON
    reason       VARCHAR(255) NOT NULL,
    rejected_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);