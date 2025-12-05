CREATE TABLE machine_sensors (
    id SERIAL PRIMARY KEY,
    machine_id VARCHAR(50) NOT NULL,
    sensor_type VARCHAR(50) NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE sensor_aggregates (
    id SERIAL PRIMARY KEY,
    machine_id VARCHAR(50) NOT NULL,
    sensor_type VARCHAR(50) NOT NULL,
    avg_value DOUBLE PRECISION,
    min_value DOUBLE PRECISION,
    max_value DOUBLE PRECISION,
    count_readings INT,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_machine_sensors_machine_ts ON machine_sensors(machine_id, ts);
CREATE INDEX idx_sensor_aggregates_machine_window ON sensor_aggregates(machine_id, window_start);

SELECT add_retention_policy('machine_sensors', INTERVAL '90 days');
