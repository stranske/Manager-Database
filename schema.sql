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
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
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

CREATE TABLE IF NOT EXISTS activism_campaigns (
    campaign_id bigserial PRIMARY KEY,
    manager_id bigint NOT NULL REFERENCES managers(manager_id),
    target_identifier text NOT NULL,
    target_company text NOT NULL,
    first_filed date NOT NULL,
    last_filed date NOT NULL,
    status text NOT NULL,
    peak_ownership_pct numeric(8,4),
    latest_ownership_pct numeric(8,4),
    filing_count integer NOT NULL DEFAULT 0,
    event_count integer NOT NULL DEFAULT 0,
    latest_event_type text,
    target_ticker text,
    window_return numeric(18,8),
    holding_period_days integer,
    return_computed_at timestamptz,
    source_forms text NOT NULL DEFAULT '[]',
    data_quality_flags text NOT NULL DEFAULT '[]',
    computed_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (manager_id, target_identifier),
    CONSTRAINT ck_activism_campaigns_status
        CHECK (status IN ('active', 'monitoring', 'closed', 'unknown'))
);
CREATE INDEX IF NOT EXISTS idx_activism_campaigns_manager ON activism_campaigns(manager_id);
CREATE INDEX IF NOT EXISTS idx_activism_campaigns_status ON activism_campaigns(status);

CREATE TABLE IF NOT EXISTS activism_campaign_timeline (
    timeline_id bigserial PRIMARY KEY,
    campaign_id bigint NOT NULL REFERENCES activism_campaigns(campaign_id),
    filing_id bigint NOT NULL REFERENCES activism_filings(filing_id),
    event_id bigint REFERENCES activism_events(event_id),
    event_date date NOT NULL,
    event_type text NOT NULL,
    form_type text NOT NULL,
    ownership_pct numeric(8,4),
    summary text NOT NULL,
    source_url text,
    UNIQUE (campaign_id, filing_id, event_id)
);
CREATE INDEX IF NOT EXISTS idx_activism_campaign_timeline_campaign
    ON activism_campaign_timeline(campaign_id, event_date);
CREATE UNIQUE INDEX IF NOT EXISTS uq_activism_campaign_timeline_filing_only
    ON activism_campaign_timeline (campaign_id, filing_id) WHERE event_id IS NULL;

CREATE TABLE IF NOT EXISTS holdings (
    holding_id bigserial PRIMARY KEY,
    filing_id bigint NOT NULL REFERENCES filings(filing_id),
    cusip text,
    isin text,
    name_of_issuer text,
    shares bigint,
    value_usd numeric(18,2),
    delta_type text,
    resolved_ticker text,
    resolved_figi text,
    resolved_lei text,
    resolution_source text,
    content_hash text,
    knowledge_time timestamptz NOT NULL DEFAULT now(),
    superseded_at timestamptz,
    version integer NOT NULL DEFAULT 1,
    created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_holdings_filing_id
    ON holdings (filing_id);

CREATE INDEX IF NOT EXISTS idx_holdings_cusip
    ON holdings (cusip);

CREATE INDEX IF NOT EXISTS idx_holdings_filing_knowledge
    ON holdings (filing_id, knowledge_time DESC, holding_id DESC);

CREATE INDEX IF NOT EXISTS idx_holdings_current_filing
    ON holdings (filing_id)
    WHERE superseded_at IS NULL;

CREATE OR REPLACE VIEW v_current_holdings AS
SELECT *
FROM holdings
WHERE superseded_at IS NULL;

CREATE TABLE IF NOT EXISTS manager_similarity (
    manager_id_a bigint NOT NULL REFERENCES managers(manager_id),
    manager_id_b bigint NOT NULL REFERENCES managers(manager_id),
    jaccard real NOT NULL,
    cosine real,
    overlap_count integer NOT NULL,
    union_count integer NOT NULL,
    computed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (manager_id_a, manager_id_b),
    CHECK (manager_id_a < manager_id_b)
);
CREATE INDEX IF NOT EXISTS idx_manager_similarity_a ON manager_similarity (manager_id_a);
CREATE INDEX IF NOT EXISTS idx_manager_similarity_b ON manager_similarity (manager_id_b);

CREATE TABLE IF NOT EXISTS insider_transactions (
    txn_id bigserial PRIMARY KEY,
    issuer_cik text NOT NULL,
    ticker text,
    insider_name text,
    txn_code text,
    shares numeric,
    txn_date date,
    acquired_disposed text,
    cusip text,
    ingested_at timestamptz DEFAULT now(),
    UNIQUE (issuer_cik, ticker, insider_name, txn_code, shares, txn_date, acquired_disposed)
);
CREATE INDEX IF NOT EXISTS idx_insider_issuer_date ON insider_transactions (issuer_cik, txn_date);
CREATE INDEX IF NOT EXISTS idx_insider_ticker_date ON insider_transactions (ticker, txn_date);

-- FINRA/exchange short-interest context for held issuers (#1470). This annotates
-- conviction output only; it must never change conviction scoring.
CREATE TABLE IF NOT EXISTS short_interest (
    metric_id bigserial PRIMARY KEY,
    ticker text NOT NULL,
    cusip text,
    short_interest numeric,
    float_shares numeric,
    short_interest_pct numeric,
    report_date date NOT NULL,
    source text NOT NULL DEFAULT 'finra',
    ingested_at timestamptz DEFAULT now(),
    UNIQUE (ticker, report_date, source)
);
CREATE INDEX IF NOT EXISTS idx_short_interest_ticker_date ON short_interest (ticker, report_date DESC);
CREATE INDEX IF NOT EXISTS idx_short_interest_cusip_date ON short_interest (cusip, report_date DESC);

-- Free-source daily closes cached for the backtest harness (#1464). Internal use
-- only: derived statistics may be surfaced, raw prices may not be redistributed.
CREATE TABLE IF NOT EXISTS price_cache (
    ticker text NOT NULL,
    price_date date NOT NULL,
    source text NOT NULL DEFAULT 'stooq',
    close_usd numeric,
    fetched_at timestamptz DEFAULT now(),
    PRIMARY KEY (ticker, price_date, source)
);
CREATE INDEX IF NOT EXISTS idx_price_cache_ticker_date ON price_cache (ticker, price_date);

CREATE TABLE IF NOT EXISTS backtest_runs (
    run_id bigserial PRIMARY KEY,
    strategy text NOT NULL,
    manager_id bigint,
    start_date date NOT NULL,
    end_date date NOT NULL,
    entry_lag_days integer NOT NULL DEFAULT 0,
    holding_period_days integer NOT NULL DEFAULT 91,
    benchmark_ticker text,
    price_source text,
    periods integer NOT NULL DEFAULT 0,
    positions integer NOT NULL DEFAULT 0,
    positions_skipped integer NOT NULL DEFAULT 0,
    total_return real,
    annualized_return real,
    sharpe real,
    hit_rate real,
    benchmark_total_return real,
    excess_return real,
    params_json text,
    created_at timestamptz DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_backtest_runs_strategy ON backtest_runs (strategy, created_at);

CREATE TABLE IF NOT EXISTS backtest_results (
    result_id bigserial PRIMARY KEY,
    run_id bigint NOT NULL REFERENCES backtest_runs(run_id) ON DELETE CASCADE,
    decision_date date NOT NULL,
    entry_date date NOT NULL,
    exit_date date NOT NULL,
    ticker text,
    cusip text,
    entry_price real,
    exit_price real,
    position_return real,
    benchmark_return real,
    excess_return real,
    weight real,
    status text NOT NULL DEFAULT 'filled',
    skip_reason text
);
CREATE INDEX IF NOT EXISTS idx_backtest_results_run ON backtest_results (run_id);

-- Position-level performance attribution since disclosure (#1465 / design #1402).
-- Derived returns may be surfaced via the signals API; raw prices are internal-use only.
CREATE TABLE IF NOT EXISTS manager_attribution (
    attribution_id bigserial PRIMARY KEY,
    manager_id bigint NOT NULL,
    filing_id bigint,
    disclosure_date date NOT NULL,
    as_of_date date NOT NULL,
    security_key text NOT NULL,
    ticker text,
    cusip text,
    name_of_issuer text,
    disclosure_price double precision,
    as_of_price double precision,
    position_return double precision,
    value_usd double precision,
    status text NOT NULL DEFAULT 'filled',
    skip_reason text,
    computed_at timestamptz DEFAULT now(),
    UNIQUE (manager_id, filing_id, security_key, as_of_date)
);
CREATE INDEX IF NOT EXISTS idx_manager_attribution_manager
    ON manager_attribution (manager_id, as_of_date);
-- NULL filing_id rows escape the UNIQUE constraint above, so they need a partial index.
CREATE UNIQUE INDEX IF NOT EXISTS uq_manager_attribution_no_filing
    ON manager_attribution (manager_id, security_key, as_of_date) WHERE filing_id IS NULL;

CREATE TABLE IF NOT EXISTS identifier_resolution_cache (
    cusip text PRIMARY KEY,
    ticker text,
    figi text,
    composite_figi text,
    share_class_figi text,
    isin text,
    lei text,
    name text,
    source text NOT NULL DEFAULT 'openfigi',
    resolved_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS identifier_resolution_metrics (
    metric_id bigserial PRIMARY KEY,
    source text NOT NULL,
    filing_id bigint,
    total_cusips integer NOT NULL,
    unmapped_cusips integer NOT NULL,
    unmapped_cusip_rate real NOT NULL,
    created_at timestamptz DEFAULT now()
);

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
    fired_at timestamptz NOT NULL DEFAULT now(),
    event_type text NOT NULL,
    payload_json jsonb NOT NULL,
    delivered_channels text[] NOT NULL DEFAULT ARRAY[]::text[],
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

CREATE TABLE IF NOT EXISTS chat_feedback (
    feedback_id bigserial PRIMARY KEY,
    response_id text NOT NULL,
    rating int NOT NULL CHECK (rating BETWEEN 1 AND 5),
    comment text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_chat_feedback_response_id
    ON chat_feedback (response_id);

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

CREATE UNIQUE INDEX IF NOT EXISTS mv_daily_report_idx
    ON mv_daily_report (report_date, manager_id, cusip, delta_type);
