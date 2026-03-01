-- Canonical data model version: 2026-02-28 (Manager Database Universe)
-- Fixes applied: idempotent materialized views, non-destructive documents
-- table creation, generated delta columns on daily_diffs.

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS managers (
    manager_id bigserial PRIMARY KEY,
    name text NOT NULL,
    aliases text[] DEFAULT '{}',
    jurisdictions text[] DEFAULT '{}',
    cik text,
    lei text,
    registry_ids jsonb DEFAULT '{}',
    tags text[] DEFAULT '{}',
    quality_flags jsonb DEFAULT '{}',
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_managers_cik_unique
    ON managers (cik)
    WHERE cik IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_managers_lei
    ON managers (lei);

CREATE TABLE IF NOT EXISTS filings (
    filing_id bigserial PRIMARY KEY,
    manager_id bigint NOT NULL REFERENCES managers(manager_id),
    type text NOT NULL,
    period_end date,
    filed_date date,
    source text NOT NULL,
    url text,
    raw_key text,
    parsed_payload jsonb,
    schema_version int DEFAULT 1,
    created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_filings_manager_filed_date
    ON filings (manager_id, filed_date);

CREATE INDEX IF NOT EXISTS idx_filings_manager_type
    ON filings (manager_id, type);

CREATE TABLE IF NOT EXISTS holdings (
    holding_id bigserial PRIMARY KEY,
    filing_id bigint NOT NULL REFERENCES filings(filing_id),
    cusip text,
    isin text,
    name_of_issuer text,
    shares bigint,
    value_usd numeric(18,2),
    delta_type text,
    created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_holdings_filing_id
    ON holdings (filing_id);

CREATE INDEX IF NOT EXISTS idx_holdings_cusip
    ON holdings (cusip);

CREATE TABLE IF NOT EXISTS news_items (
    news_id bigserial PRIMARY KEY,
    manager_id bigint REFERENCES managers(manager_id),
    published_at timestamptz NOT NULL,
    source text NOT NULL,
    headline text NOT NULL,
    url text,
    body_snippet text,
    topics text[] DEFAULT '{}',
    confidence real,
    created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_news_items_manager_published_at
    ON news_items (manager_id, published_at);

CREATE INDEX IF NOT EXISTS idx_news_items_topics_gin
    ON news_items USING GIN (topics);

CREATE TABLE IF NOT EXISTS documents (
    doc_id bigserial PRIMARY KEY,
    manager_id bigint REFERENCES managers(manager_id),
    kind text NOT NULL DEFAULT 'note',
    filename text,
    sha256 text,
    text text,
    embedding vector(384),
    created_at timestamptz DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_documents_sha256_unique
    ON documents (sha256)
    WHERE sha256 IS NOT NULL;

CREATE TABLE IF NOT EXISTS daily_diffs (
    diff_id bigserial PRIMARY KEY,
    manager_id bigint NOT NULL REFERENCES managers(manager_id),
    report_date date NOT NULL,
    cusip text NOT NULL,
    name_of_issuer text,
    delta_type text NOT NULL,
    shares_prev bigint,
    shares_curr bigint,
    shares_delta bigint GENERATED ALWAYS AS (shares_curr - shares_prev) STORED,
    value_prev numeric(18,2),
    value_curr numeric(18,2),
    value_delta numeric(18,2) GENERATED ALWAYS AS (value_curr - value_prev) STORED,
    created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_daily_diffs_report_date_manager
    ON daily_diffs (report_date, manager_id);

CREATE TABLE IF NOT EXISTS api_usage (
    id bigserial PRIMARY KEY,
    ts timestamptz DEFAULT now(),
    source text,
    endpoint text,
    status int,
    bytes int,
    latency_ms int,
    cost_usd numeric(10,4)
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_matviews WHERE matviewname = 'monthly_usage'
  ) THEN
    EXECUTE $mv$
      CREATE MATERIALIZED VIEW monthly_usage AS
      SELECT date_trunc('month', ts) AS month,
             source,
             count(*)        AS calls,
             sum(bytes)      AS total_bytes,
             sum(cost_usd)   AS total_cost
      FROM api_usage
      GROUP BY 1, 2
    $mv$;
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_matviews WHERE matviewname = 'mv_daily_report'
  ) THEN
    EXECUTE $mv$
      CREATE MATERIALIZED VIEW mv_daily_report AS
      SELECT
          d.report_date,
          m.manager_id,
          m.name           AS manager_name,
          d.cusip,
          d.name_of_issuer,
          d.delta_type,
          d.shares_prev,
          d.shares_curr,
          (d.shares_curr - d.shares_prev) AS shares_delta,
          d.value_prev,
          d.value_curr,
          (d.value_curr - d.value_prev)   AS value_delta
      FROM daily_diffs d
      JOIN managers m ON m.manager_id = d.manager_id
      ORDER BY d.report_date DESC, m.name, d.delta_type
    $mv$;
  END IF;
END
$$;
