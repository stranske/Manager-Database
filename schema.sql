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
