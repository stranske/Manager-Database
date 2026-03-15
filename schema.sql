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

CREATE UNIQUE INDEX IF NOT EXISTS idx_filings_raw_key_unique
    ON filings (raw_key)
    WHERE raw_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_filings_manager_filed_date
    ON filings (manager_id, filed_date);

CREATE INDEX IF NOT EXISTS idx_filings_manager_type
    ON filings (manager_id, type);

CREATE TABLE IF NOT EXISTS activism_filings (
    filing_id bigserial PRIMARY KEY,
    manager_id bigint NOT NULL REFERENCES managers(manager_id),
    filing_type text NOT NULL CHECK (
        filing_type IN ('SC 13D', 'SC 13D/A', 'SC 13G', 'SC 13G/A')
    ),
    subject_company text NOT NULL,
    subject_cusip text,
    ownership_pct numeric(8,4),
    shares bigint,
    group_members text[] DEFAULT '{}',
    purpose_snippet text,
    filed_date date NOT NULL,
    url text NOT NULL,
    raw_key text,
    created_at timestamptz DEFAULT now(),
    UNIQUE (manager_id, filing_type, subject_cusip, filed_date)
);

CREATE INDEX IF NOT EXISTS idx_activism_manager
    ON activism_filings (manager_id);

CREATE INDEX IF NOT EXISTS idx_activism_cusip
    ON activism_filings (subject_cusip);

CREATE INDEX IF NOT EXISTS idx_activism_date
    ON activism_filings (filed_date DESC);

CREATE TABLE IF NOT EXISTS activism_events (
    event_id bigserial PRIMARY KEY,
    manager_id bigint NOT NULL REFERENCES managers(manager_id),
    filing_id bigint NOT NULL REFERENCES activism_filings(filing_id),
    event_type text NOT NULL CHECK (
        event_type IN (
            'initial_stake',
            'threshold_crossing',
            'stake_increase',
            'stake_decrease',
            'group_formation',
            'amendment',
            'form_upgrade',
            'form_downgrade'
        )
    ),
    subject_company text NOT NULL,
    subject_cusip text,
    ownership_pct numeric(8,4),
    previous_pct numeric(8,4),
    delta_pct numeric(8,4),
    threshold_crossed numeric(8,4),
    detected_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_activism_events_manager
    ON activism_events (manager_id);

CREATE INDEX IF NOT EXISTS idx_activism_events_type
    ON activism_events (event_type);

CREATE INDEX IF NOT EXISTS idx_activism_events_date
    ON activism_events (detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_activism_events_cusip
    ON activism_events (subject_cusip);

CREATE UNIQUE INDEX IF NOT EXISTS idx_activism_events_unique_base
    ON activism_events (manager_id, filing_id, event_type)
    WHERE threshold_crossed IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_activism_events_unique_threshold
    ON activism_events (manager_id, filing_id, event_type, threshold_crossed)
    WHERE threshold_crossed IS NOT NULL;

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

CREATE TABLE IF NOT EXISTS alert_rules (
    rule_id bigserial PRIMARY KEY,
    name text NOT NULL,
    description text,
    event_type text NOT NULL CHECK (
        event_type IN (
            'new_filing',
            'large_delta',
            'news_spike',
            'crowded_trade_change',
            'contrarian_signal',
            'missing_filing',
            'etl_failure',
            'activism_event'
        )
    ),
    condition_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    channels text[] NOT NULL DEFAULT ARRAY['streamlit'],
    enabled boolean NOT NULL DEFAULT true,
    manager_id bigint REFERENCES managers(manager_id),
    created_by text,
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_alert_rules_event
    ON alert_rules (event_type)
    WHERE enabled = true;

CREATE TABLE IF NOT EXISTS alert_history (
    alert_id bigserial PRIMARY KEY,
    rule_id bigint NOT NULL REFERENCES alert_rules(rule_id),
    fired_at timestamptz DEFAULT now(),
    event_type text NOT NULL,
    payload_json jsonb NOT NULL,
    delivered_channels text[] DEFAULT ARRAY[]::text[],
    delivery_errors jsonb,
    acknowledged boolean NOT NULL DEFAULT false,
    acknowledged_by text,
    acknowledged_at timestamptz
);

CREATE INDEX IF NOT EXISTS idx_alert_history_unack
    ON alert_history (fired_at DESC)
    WHERE acknowledged = false;

CREATE INDEX IF NOT EXISTS idx_alert_history_rule
    ON alert_history (rule_id);

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
    SELECT 1 FROM pg_matviews
    WHERE schemaname = current_schema() AND matviewname = 'monthly_usage'
  ) THEN
    EXECUTE $mv$
      CREATE MATERIALIZED VIEW monthly_usage AS
      SELECT date_trunc('month', ts) AS month,
             source,
             count(*)        AS calls,
             sum(bytes)      AS mb,
             sum(cost_usd)   AS cost
      FROM api_usage
      GROUP BY 1, 2
    $mv$;
  END IF;
END
$$;

CREATE TABLE IF NOT EXISTS conviction_scores (
    score_id bigserial PRIMARY KEY,
    manager_id bigint NOT NULL REFERENCES managers(manager_id),
    filing_id bigint NOT NULL REFERENCES filings(filing_id),
    cusip text NOT NULL,
    name_of_issuer text,
    shares bigint,
    value_usd numeric(16,2),
    conviction_pct numeric(8,4),
    portfolio_weight numeric(8,6),
    computed_at timestamptz DEFAULT now(),
    UNIQUE (filing_id, cusip)
);

CREATE INDEX IF NOT EXISTS idx_conviction_manager ON conviction_scores(manager_id);
CREATE INDEX IF NOT EXISTS idx_conviction_cusip ON conviction_scores(cusip);
CREATE INDEX IF NOT EXISTS idx_conviction_pct ON conviction_scores(conviction_pct DESC);

CREATE TABLE IF NOT EXISTS crowded_trades (
    crowd_id bigserial PRIMARY KEY,
    cusip text NOT NULL,
    name_of_issuer text,
    manager_count int NOT NULL,
    manager_ids bigint[] NOT NULL,
    total_value_usd numeric(18,2),
    avg_conviction_pct numeric(8,4),
    max_conviction_pct numeric(8,4),
    report_date date NOT NULL,
    computed_at timestamptz DEFAULT now(),
    UNIQUE (cusip, report_date)
);

CREATE INDEX IF NOT EXISTS idx_crowded_date ON crowded_trades(report_date DESC);
CREATE INDEX IF NOT EXISTS idx_crowded_count ON crowded_trades(manager_count DESC);

CREATE TABLE IF NOT EXISTS contrarian_signals (
    signal_id bigserial PRIMARY KEY,
    manager_id bigint NOT NULL REFERENCES managers(manager_id),
    cusip text NOT NULL,
    name_of_issuer text,
    direction text NOT NULL CHECK (direction IN ('BUY', 'SELL', 'INCREASE', 'DECREASE')),
    consensus_direction text NOT NULL CHECK (
        consensus_direction IN ('BUY', 'SELL', 'INCREASE', 'DECREASE', 'HOLD')
    ),
    manager_delta_shares bigint,
    manager_delta_value numeric(16,2),
    consensus_count int,
    report_date date NOT NULL,
    detected_at timestamptz DEFAULT now(),
    UNIQUE (manager_id, cusip, report_date)
);

CREATE INDEX IF NOT EXISTS idx_contrarian_manager ON contrarian_signals(manager_id);
CREATE INDEX IF NOT EXISTS idx_contrarian_date ON contrarian_signals(report_date DESC);

CREATE UNIQUE INDEX IF NOT EXISTS mv_daily_report_idx
    ON mv_daily_report (report_date, manager_id, cusip, delta_type);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_matviews
    WHERE schemaname = current_schema() AND matviewname = 'mv_daily_report'
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
