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

CREATE INDEX idx_crowded_date ON crowded_trades(report_date DESC);
CREATE INDEX idx_crowded_count ON crowded_trades(manager_count DESC);
