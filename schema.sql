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

CREATE INDEX idx_conviction_manager ON conviction_scores(manager_id);
CREATE INDEX idx_conviction_cusip ON conviction_scores(cusip);
CREATE INDEX idx_conviction_pct ON conviction_scores(conviction_pct DESC);
