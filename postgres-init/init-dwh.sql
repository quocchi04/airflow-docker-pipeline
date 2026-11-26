CREATE DATABASE dwh;

\c dwh

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS raw.coin_prices (
    id                          TEXT,
    symbol                      TEXT,
    name                        TEXT,
    current_price               NUMERIC,
    market_cap                  NUMERIC,
    market_cap_rank             INT,
    total_volume                NUMERIC,
    price_change_percentage_24h NUMERIC,
    last_updated                TIMESTAMPTZ,
    -- ↓ Metadata do pipeline thêm vào
    snapshot_ts                 TIMESTAMPTZ NOT NULL,
    collected_date              DATE        NOT NULL,
    source_file                 TEXT        NOT NULL,
    execution_time              TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_coin_prices_collected_date ON raw.coin_prices (collected_date);

CREATE TABLE IF NOT EXISTS raw.s3_file_log (
    s3_key         TEXT PRIMARY KEY,
    collected_date DATE        NOT NULL,
    row_count      INT,
    execution_time TIMESTAMPTZ NOT NULL
);
