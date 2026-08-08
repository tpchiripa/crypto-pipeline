-- Raw landing table: every event that came off Kafka, untouched.
-- Keeping this is what lets you replay/reprocess if you fix a normalizer bug
-- later, or add a new derived table without re-pulling from the source.
CREATE TABLE IF NOT EXISTS raw_events (
    id              BIGSERIAL PRIMARY KEY,
    source_id       TEXT NOT NULL,
    source_type     TEXT NOT NULL,
    schema_version  TEXT NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL,
    payload         JSONB NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_raw_events_source ON raw_events (source_id, ingested_at);

-- Normalized, source-specific table: clean, typed, queryable.
-- This is what the API and any downstream consumer actually reads from.
CREATE TABLE IF NOT EXISTS crypto_trades (
    id              BIGSERIAL PRIMARY KEY,
    symbol          TEXT NOT NULL,
    price           NUMERIC(20, 8) NOT NULL,
    quantity        NUMERIC(20, 8) NOT NULL,
    trade_time      TIMESTAMPTZ NOT NULL,
    is_buyer_maker  BOOLEAN NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_crypto_trades_symbol_time ON crypto_trades (symbol, trade_time DESC);
