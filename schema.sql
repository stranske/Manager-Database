CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS managers (
    id bigserial PRIMARY KEY,
    name text NOT NULL,
    cik text NOT NULL UNIQUE,
    role text,
    department text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS manager_aliases (
    id bigserial PRIMARY KEY,
    manager_id bigint NOT NULL REFERENCES managers(id),
    alias text NOT NULL,
    UNIQUE (manager_id, alias)
);

CREATE TABLE IF NOT EXISTS manager_jurisdictions (
    id bigserial PRIMARY KEY,
    manager_id bigint NOT NULL REFERENCES managers(id),
    jurisdiction text NOT NULL,
    UNIQUE (manager_id, jurisdiction)
);

CREATE TABLE IF NOT EXISTS manager_tags (
    id bigserial PRIMARY KEY,
    manager_id bigint NOT NULL REFERENCES managers(id),
    tag text NOT NULL,
    UNIQUE (manager_id, tag)
);

CREATE TABLE IF NOT EXISTS filings (
    id bigserial PRIMARY KEY,
    manager_id bigint NOT NULL REFERENCES managers(id),
    accession text NOT NULL UNIQUE,
    filed date NOT NULL,
    form_type text NOT NULL DEFAULT '13F-HR',
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS holdings (
    id bigserial PRIMARY KEY,
    filing_id bigint NOT NULL REFERENCES filings(id),
    manager_id bigint NOT NULL REFERENCES managers(id),
    name_of_issuer text,
    cusip text NOT NULL,
    value bigint,
    ssh_prnamt bigint,
    UNIQUE (filing_id, cusip)
);

CREATE TABLE IF NOT EXISTS api_usage (
    id bigserial PRIMARY KEY,
    ts timestamptz NOT NULL DEFAULT now(),
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
       count(*) AS calls,
       sum(bytes) AS mb,
       sum(cost_usd) AS cost
FROM api_usage
GROUP BY 1,2;

CREATE MATERIALIZED VIEW IF NOT EXISTS manager_holdings_summary AS
SELECT m.id AS manager_id,
       m.name AS manager_name,
       max(f.filed) AS latest_filed,
       count(h.id) AS holdings_count,
       coalesce(sum(h.value), 0) AS total_value
FROM managers m
LEFT JOIN filings f ON f.manager_id = m.id
LEFT JOIN holdings h ON h.filing_id = f.id
GROUP BY m.id, m.name;
