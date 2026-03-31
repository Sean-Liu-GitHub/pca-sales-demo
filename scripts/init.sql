-- This script runs automatically on first postgres startup.
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- Raw layer: dump from Kafka consumer, no transformation
CREATE TABLE IF NOT EXISTS raw.policy_sales (
    event_id         UUID           PRIMARY KEY,
    event_type       VARCHAR(50)    NOT NULL,
    event_ts         TIMESTAMPTZ    NOT NULL,
    policy_id        UUID           NOT NULL,
    agent_id         VARCHAR(20)    NOT NULL,
    product_code     VARCHAR(30)    NOT NULL,
    product_type     VARCHAR(30)    NOT NULL,
    region           VARCHAR(50)    NOT NULL,
    channel          VARCHAR(20)    NOT NULL,
    premium_amount   NUMERIC(12,2)  NOT NULL,
    sum_assured      NUMERIC(15,2)  NOT NULL,
    customer_age_band VARCHAR(10)   NOT NULL,
    payment_frequency VARCHAR(20)   NOT NULL,
    ingested_at      TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);
 
-- Index for time-based queries (dashboard reads by recent time windows)
CREATE INDEX IF NOT EXISTS idx_policy_sales_event_ts
    ON raw.policy_sales (event_ts DESC);
 
-- Index for agent-based queries (leaderboard)
CREATE INDEX IF NOT EXISTS idx_policy_sales_agent_id
    ON raw.policy_sales (agent_id);