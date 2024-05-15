-- #TODO: Create new TS hypertable

CREATE TABLE IF NOT EXISTS sensor_data (
    id SERIAL PRIMARY KEY,
    temperature DOUBLE PRECISION NOT NULL,
    humidity DOUBLE PRECISION NOT NULL,
    battery_level DOUBLE PRECISION NOT NULL,
    last_seen TIMESTAMP NOT NULL
);
SELECT create_hypertable('sensor_data', 'last_seen');