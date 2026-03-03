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

CREATE MATERIALIZED VIEW IF NOT EXISTS monthly_usage AS
SELECT date_trunc('month', ts) AS month,
       source,
       count(*)  AS calls,
       sum(bytes) AS mb,
       sum(cost_usd) AS cost
FROM api_usage
GROUP BY 1,2;

CREATE TABLE IF NOT EXISTS documents (
    id bigserial PRIMARY KEY,
    content text,
    embedding double precision[]
);

CREATE TABLE IF NOT EXISTS alert_rules (
    rule_id         bigserial PRIMARY KEY,
    name            text NOT NULL,
    description     text,
    event_type      text NOT NULL CHECK (event_type IN (
        'new_filing', 'large_delta', 'news_spike', 'crowded_trade_change',
        'contrarian_signal', 'missing_filing', 'etl_failure', 'activism_event'
    )),
    condition_json  jsonb NOT NULL DEFAULT '{}',
    channels        text[] NOT NULL DEFAULT ARRAY['streamlit'],
    enabled         boolean NOT NULL DEFAULT true,
    manager_id      bigint REFERENCES managers(manager_id),
    created_by      text,
    created_at      timestamptz DEFAULT now(),
    updated_at      timestamptz DEFAULT now()
);

CREATE INDEX idx_alert_rules_event ON alert_rules(event_type) WHERE enabled = true;
