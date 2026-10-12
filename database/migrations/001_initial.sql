CREATE TABLE IF NOT EXISTS transactions (
    transaction_id text PRIMARY KEY,
    source_event_id text NOT NULL UNIQUE,
    customer_id text NOT NULL,
    merchant_id text NOT NULL,
    event_timestamp timestamptz NOT NULL,
    accepted_at timestamptz NOT NULL,
    event jsonb NOT NULL,
    correlation_id text NOT NULL
);
CREATE INDEX IF NOT EXISTS transactions_recent ON transactions (accepted_at DESC);
CREATE TABLE IF NOT EXISTS processing_state (
    sequence bigserial UNIQUE NOT NULL,
    transaction_id text PRIMARY KEY REFERENCES transactions(transaction_id),
    features jsonb NOT NULL,
    feature_version text NOT NULL,
    prepared_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    feature_materialized_at timestamptz,
    feature_seconds double precision NOT NULL,
    materialized boolean NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS processing_pending ON processing_state(sequence) WHERE NOT materialized;
CREATE TABLE IF NOT EXISTS model_versions (
    model_version text PRIMARY KEY,
    registered_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS decisions (
    transaction_id text PRIMARY KEY REFERENCES transactions(transaction_id),
    risk_score double precision NOT NULL CHECK (risk_score >= 0 AND risk_score <= 1),
    decision text NOT NULL CHECK (decision IN ('ALLOW','REVIEW','BLOCK')),
    model_version text NOT NULL REFERENCES model_versions(model_version),
    scoring_timestamp timestamptz NOT NULL,
    persisted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    inference_ms double precision NOT NULL,
    feature_ms double precision NOT NULL,
    precommit_ms double precision NOT NULL
);
CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id text PRIMARY KEY,
    transaction_id text NOT NULL UNIQUE REFERENCES decisions(transaction_id),
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    observed_at timestamptz
);
CREATE TABLE IF NOT EXISTS outbox (
    event_id text PRIMARY KEY,
    topic text NOT NULL,
    message_key text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at timestamptz,
    attempts integer NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(created_at) WHERE published_at IS NULL;
CREATE TABLE IF NOT EXISTS quarantined_events (
    topic text NOT NULL,
    partition integer NOT NULL,
    event_offset bigint NOT NULL,
    reason text NOT NULL,
    payload text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (topic, partition, event_offset)
);
CREATE TABLE IF NOT EXISTS fraud_labels (
    transaction_id text PRIMARY KEY,
    fraud boolean NOT NULL,
    available_at timestamptz NOT NULL
);
