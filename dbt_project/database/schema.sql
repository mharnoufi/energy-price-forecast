CREATE TABLE IF NOT EXISTS raw_prices (
    timestamp_utc TIMESTAMPTZ PRIMARY KEY,
    price_eur_mwh FLOAT8 NOT NULL,
    country_code TEXT NOT NULL DEFAULT 'FR'
);

CREATE TABLE IF NOT EXISTS raw_weather (
    timestamp_utc TIMESTAMPTZ NOT NULL,
    city TEXT NOT NULL,
    temp FLOAT8,
    temp_app FLOAT8,
    hum FLOAT8,
    precip FLOAT8,
    clouds FLOAT8,
    press FLOAT8,
    w_speed_100 FLOAT8,
    w_dir_100 FLOAT8,
    rad_short FLOAT8,
    rad_dir FLOAT8,
    rad_diff FLOAT8,
    dni FLOAT8,
    PRIMARY KEY (timestamp_utc, city)
);

CREATE INDEX IF NOT EXISTS idx_weather_timestamp ON raw_weather(timestamp_utc);
CREATE INDEX IF NOT EXISTS idx_prices_timestamp ON raw_prices(timestamp_utc);