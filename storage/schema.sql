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

-- Second source, deliberately unlike crypto_trades: text content, no fixed
-- numeric fields, dedup on a stable article id instead of accepting every
-- message as a new fact.
CREATE TABLE IF NOT EXISTS news_articles (
    id              BIGSERIAL PRIMARY KEY,
    article_id      TEXT NOT NULL UNIQUE,
    title           TEXT NOT NULL,
    summary         TEXT,
    link            TEXT,
    author          TEXT,
    published_raw   TEXT,
    ingested_at     TIMESTAMPTZ NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_news_articles_received ON news_articles (received_at DESC);

-- Third source, deliberately unlike the other two: batch file ingestion
-- rather than a live API, and genuinely messy input. Most fields are
-- nullable on purpose - the parser does its best to clean what it can
-- (currency symbols, multiple date formats) but real POS/accounting
-- exports won't always have every field, and raw_row keeps the original
-- data around so nothing's silently lost even when a field can't be parsed.
CREATE TABLE IF NOT EXISTS retail_transactions (
    id                  BIGSERIAL PRIMARY KEY,
    product_name        TEXT NOT NULL,
    store_id            TEXT,
    quantity            INTEGER,
    unit_price          NUMERIC(12, 2),
    total_amount        NUMERIC(12, 2),
    transaction_date    TIMESTAMPTZ,
    payment_method      TEXT,
    source_file         TEXT,
    source_row          INTEGER,
    raw_row             JSONB,
    ingested_at         TIMESTAMPTZ NOT NULL,
    received_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_retail_transactions_date ON retail_transactions (transaction_date DESC);
CREATE INDEX IF NOT EXISTS idx_retail_transactions_product ON retail_transactions (product_name);
