-- 1. Latest reading per customer
SELECT DISTINCT ON (customer_id)
       customer_id, heart_rate, status, recorded_at
FROM heartbeat_readings
ORDER BY customer_id, recorded_at DESC;

-- 2. Average heart rate per customer, per minute (last hour)
SELECT customer_id,
       date_trunc('minute', recorded_at) AS minute,
       ROUND(AVG(heart_rate), 1)        AS avg_bpm
FROM heartbeat_readings
WHERE recorded_at >= NOW() - INTERVAL '1 hour'
GROUP BY customer_id, minute
ORDER BY minute DESC, customer_id;

-- 3. Anomalies in the last 15 minutes
SELECT customer_id, heart_rate, status, recorded_at
FROM heartbeat_readings
WHERE status <> 'normal'
  AND recorded_at >= NOW() - INTERVAL '15 minutes'
ORDER BY recorded_at DESC;

-- 4. How many readings fall into each status
SELECT status, COUNT(*) AS readings
FROM heartbeat_readings
GROUP BY status;

-- 5. Pipeline lag: time from sensor to database
SELECT ROUND(AVG(EXTRACT(EPOCH FROM (ingested_at - recorded_at)))::numeric, 3) AS avg_lag_seconds,
       ROUND(MAX(EXTRACT(EPOCH FROM (ingested_at - recorded_at)))::numeric, 3) AS max_lag_seconds
FROM heartbeat_readings;

-- 6. Rejected messages and why
SELECT reason, COUNT(*) AS total
FROM heartbeat_rejected
GROUP BY reason
ORDER BY total DESC;