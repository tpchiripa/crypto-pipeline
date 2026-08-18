-- Multi-tenancy: every user belongs to exactly one organization. This is
-- the tenant boundary - data that's genuinely private to one business
-- (retail_transactions) gets scoped to org_id; data that's shared public
-- fact (crypto prices, news) stays open and isn't tenant-scoped at all.
CREATE TABLE IF NOT EXISTS organizations (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    slug        TEXT NOT NULL UNIQUE,
    -- Drives which dashboard panels are relevant to this org - NOT a
    -- security boundary (crypto/news stay public regardless), just
    -- personalization: a retail business shouldn't have to look at a
    -- crypto ticker it has no use for, and vice versa.
    industry    TEXT NOT NULL DEFAULT 'general' CHECK (industry IN ('retail', 'hospitality', 'crypto', 'general')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS users (
    id              BIGSERIAL PRIMARY KEY,
    org_id          BIGINT NOT NULL REFERENCES organizations(id),
    email           TEXT NOT NULL UNIQUE,
    password_hash   TEXT NOT NULL,
    role            TEXT NOT NULL DEFAULT 'member',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_users_org ON users (org_id);

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
    feed_source     TEXT,
    ingested_at     TIMESTAMPTZ NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_news_articles_received ON news_articles (received_at DESC);
CREATE INDEX IF NOT EXISTS idx_news_articles_feed ON news_articles (feed_source);

-- Third source, deliberately unlike the other two: batch file ingestion
-- rather than a live API, and genuinely messy input. Most fields are
-- nullable on purpose - the parser does its best to clean what it can
-- (currency symbols, multiple date formats) but real POS/accounting
-- exports won't always have every field, and raw_row keeps the original
-- data around so nothing's silently lost even when a field can't be parsed.
--
-- org_id is the tenant boundary: retail data is genuinely private to one
-- business, unlike crypto prices or news articles which are shared public
-- data with no meaningful notion of "ownership". Multi-tenancy belongs
-- specifically here, not bolted onto every table for its own sake.
CREATE TABLE IF NOT EXISTS retail_transactions (
    id                  BIGSERIAL PRIMARY KEY,
    org_id              BIGINT REFERENCES organizations(id),
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
CREATE INDEX IF NOT EXISTS idx_retail_transactions_org ON retail_transactions (org_id, received_at DESC);

-- GL reconciliation: the canonical chart of accounts each org reports
-- against (typically mirrors their accounting system, e.g. Xero). This is
-- the "single source of truth" every other system's categories get
-- translated into.
CREATE TABLE IF NOT EXISTS gl_accounts (
    id              BIGSERIAL PRIMARY KEY,
    org_id          BIGINT NOT NULL REFERENCES organizations(id),
    code            TEXT NOT NULL,
    name            TEXT NOT NULL,
    account_type    TEXT NOT NULL DEFAULT 'expense' CHECK (account_type IN ('cogs', 'revenue', 'expense', 'asset', 'liability')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (org_id, code)
);

-- The crosswalk itself: maps one source system's native category name
-- to one canonical gl_account. This is what a human would otherwise be
-- doing by hand every reporting period - "Dyner's 'Draft Beer' = our
-- COGS Beverage account". Once set, every future transaction with that
-- (source_system, source_category) pair resolves automatically.
CREATE TABLE IF NOT EXISTS gl_mappings (
    id                  BIGSERIAL PRIMARY KEY,
    org_id              BIGINT NOT NULL REFERENCES organizations(id),
    source_system       TEXT NOT NULL,   -- 'dyner' | 'lightspeed' | 'xero' | ...
    source_category     TEXT NOT NULL,   -- that system's own label, verbatim
    gl_account_id       BIGINT NOT NULL REFERENCES gl_accounts(id),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (org_id, source_system, source_category)
);

-- Staging table for line items from ANY connected system (ordering, POS,
-- accounting). canonical_gl_account_id starts NULL and gets filled in by
-- the normalizer via gl_mappings at ingest time. Rows that stay NULL are
-- genuinely unmapped - surfaced for review rather than silently dropped
-- or guessed at, since guessing wrong in accounting data is worse than
-- flagging it.
CREATE TABLE IF NOT EXISTS gl_transactions (
    id                      BIGSERIAL PRIMARY KEY,
    org_id                  BIGINT NOT NULL REFERENCES organizations(id),
    source_system           TEXT NOT NULL,
    source_category         TEXT NOT NULL,
    canonical_gl_account_id BIGINT REFERENCES gl_accounts(id),
    description             TEXT,
    amount                  NUMERIC(14, 2),
    transaction_date        TIMESTAMPTZ,
    source_file             TEXT,
    raw_row                 JSONB,
    ingested_at             TIMESTAMPTZ NOT NULL,
    received_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_gl_transactions_org ON gl_transactions (org_id, received_at DESC);
CREATE INDEX IF NOT EXISTS idx_gl_transactions_unmapped ON gl_transactions (org_id) WHERE canonical_gl_account_id IS NULL;

-- Fifth source: hospitality ingredient/inventory data. The core problem
-- this solves is unit inconsistency - a produce supplier invoices in kg,
-- a butcher in lbs, a bar in "each" bottle. Both the raw values AND a
-- standardized version are kept: raw for audit/trust, standardized so
-- "how much beef did we use this month" is a single honest SUM() instead
-- of a mix of incompatible units.
CREATE TABLE IF NOT EXISTS hospitality_inventory (
    id                  BIGSERIAL PRIMARY KEY,
    org_id              BIGINT REFERENCES organizations(id),
    ingredient_name     TEXT NOT NULL,
    category            TEXT,
    quantity_raw        NUMERIC(14, 4),
    unit_raw            TEXT,
    quantity_standard   NUMERIC(14, 4),
    unit_standard       TEXT,        -- 'g' | 'ml' | 'each'
    unit_dimension      TEXT,        -- 'mass' | 'volume' | 'count' | 'unknown'
    cost                NUMERIC(12, 2),
    supplier            TEXT,
    transaction_date    TIMESTAMPTZ,
    source_file         TEXT,
    source_row          INTEGER,
    raw_row             JSONB,
    ingested_at         TIMESTAMPTZ NOT NULL,
    received_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_hospitality_org ON hospitality_inventory (org_id, received_at DESC);
CREATE INDEX IF NOT EXISTS idx_hospitality_ingredient ON hospitality_inventory (ingredient_name);
