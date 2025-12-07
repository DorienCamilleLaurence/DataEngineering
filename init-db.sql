-----------------------------------------------------
-- RAW SENSOR DATA (hypertable)
-----------------------------------------------------
CREATE TABLE IF NOT EXISTS machine_sensors (
    machine_id      TEXT NOT NULL,
    machine_type    TEXT,
    location        TEXT,
    sensor_type     TEXT NOT NULL,
    value           DOUBLE PRECISION NOT NULL,
    ts              TIMESTAMPTZ NOT NULL
);

-- Maak er een hypertable van
SELECT create_hypertable('machine_sensors', 'ts', if_not_exists => TRUE);

-- Retention werkt nu WEL
SELECT add_retention_policy('machine_sensors', INTERVAL '90 days');


-----------------------------------------------------
-- AGGREGATED SENSOR DATA (hypertable)
-----------------------------------------------------
CREATE TABLE IF NOT EXISTS sensor_aggregates (
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    machine_id      TEXT NOT NULL,
    sensor_type     TEXT NOT NULL,
    avg_value       DOUBLE PRECISION,
    min_value       DOUBLE PRECISION,
    max_value       DOUBLE PRECISION,
    reading_count   BIGINT
);


-- Maak hypertable (met window_start als tijdkolom)
SELECT create_hypertable('sensor_aggregates', 'window_start', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_machine_sensors_ts
    ON machine_sensors(machine_id, ts DESC);

CREATE INDEX IF NOT EXISTS idx_sensor_aggregates_window
    ON sensor_aggregates(machine_id, window_start DESC);
