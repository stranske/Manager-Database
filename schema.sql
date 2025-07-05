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
