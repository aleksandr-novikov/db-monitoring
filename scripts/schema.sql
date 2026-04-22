-- DB Monitoring — schema for monitored Supabase database
-- 4 tables: users, products, orders, events
-- ML focus: 10 detection cases, 4 methods (STL/Prophet, Isolation Forest, Z-score, PSI/PELT)
--
-- Run in Supabase SQL Editor or via psql:
--   psql $DATABASE_URL -f scripts/schema.sql

-- ────────────────────────────────────────────
-- users
-- ML кейсы:
--   #8  email       → null_rate spike   (Rolling Z-score / pandas)
--   #9  signup_source → concept drift   (PSI / KS-test / evidently)
-- ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS users (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    email          TEXT,                               -- nullable: null_rate аномалия (#8)
    age            INTEGER,
    country        TEXT        NOT NULL DEFAULT 'RU',
    signup_source  TEXT        NOT NULL                -- drift: web/mobile/api/referral (#9)
                       CHECK (signup_source IN ('web', 'mobile', 'api', 'referral')),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_users_created_at    ON users (created_at);
CREATE INDEX IF NOT EXISTS idx_users_signup_source ON users (signup_source);


-- ────────────────────────────────────────────
-- products
-- ML кейсы:
--   #5  stock + avg_daily_sales → stockout forecast  (Prophet / SARIMA)
--   #6  return_rate             → changepoint         (PELT / ruptures)
--   #7  price_updated_at        → update spike        (Rolling count / pandas)
-- ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS products (
    id               UUID           PRIMARY KEY DEFAULT gen_random_uuid(),
    name             TEXT           NOT NULL,
    category         TEXT           NOT NULL,
    price            NUMERIC(12, 2) NOT NULL,
    cost_price       NUMERIC(12, 2) NOT NULL,
    stock            INTEGER        NOT NULL DEFAULT 0,       -- forecast: days_until_stockout (#5)
    avg_daily_sales  DOUBLE PRECISION NOT NULL DEFAULT 0,    -- Prophet/SARIMA input (#5)
    return_rate      DOUBLE PRECISION NOT NULL DEFAULT 0     -- [0,1] PELT changepoint (#6)
                         CHECK (return_rate >= 0 AND return_rate <= 1),
    price_updated_at TIMESTAMPTZ,                            -- rolling count spike (#7)
    created_at       TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_products_category       ON products (category);
CREATE INDEX IF NOT EXISTS idx_products_price_updated  ON products (price_updated_at);


-- ────────────────────────────────────────────
-- orders
-- ML кейсы:
--   #1  hour_of_day + day_of_week  → seasonality outlier  (STL + 3σ / statsmodels)
--   #2  amount + items_count       → multivariate outlier  (Isolation Forest / sklearn)
--   #3  user_orders_last_1h        → velocity spike        (Z-score rolling / pandas)
--   #4  amount_vs_avg_ratio        → user-profile outlier  (Isolation Forest / sklearn)
-- ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS orders (
    id                  UUID           PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id             UUID           NOT NULL REFERENCES users (id),
    amount              NUMERIC(12, 2) NOT NULL,             -- multivariate (#2), outlier (#4)
    items_count         INTEGER        NOT NULL,             -- multivariate (#2)
    discount            NUMERIC(6, 4)  NOT NULL DEFAULT 0,
    shipping_country    TEXT           NOT NULL DEFAULT 'RU',
    status              TEXT           NOT NULL DEFAULT 'confirmed'
                            CHECK (status IN ('pending','confirmed','shipped','delivered','cancelled')),
    has_prior_events    BOOLEAN        NOT NULL DEFAULT TRUE,
    user_orders_last_1h INTEGER        NOT NULL DEFAULT 0,   -- velocity (#3)
    amount_vs_avg_ratio DOUBLE PRECISION,                    -- user-profile outlier (#4)
    hour_of_day         SMALLINT       NOT NULL              -- seasonality STL (#1)
                            GENERATED ALWAYS AS (EXTRACT(HOUR FROM created_at AT TIME ZONE 'UTC')::SMALLINT) STORED,
    day_of_week         SMALLINT       NOT NULL              -- seasonality STL (#1), 0=Sun
                            GENERATED ALWAYS AS (EXTRACT(DOW FROM created_at AT TIME ZONE 'UTC')::SMALLINT) STORED,
    created_at          TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_orders_user_id    ON orders (user_id);
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders (created_at);
CREATE INDEX IF NOT EXISTS idx_orders_hour_dow   ON orders (hour_of_day, day_of_week);


-- ────────────────────────────────────────────
-- events
-- ML кейсы:
--   #10 server_id distribution → load-balancer drift  (PSI / evidently)
-- ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS events (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id           UUID        NOT NULL REFERENCES users (id),
    session_id        UUID        NOT NULL,
    event_type        TEXT        NOT NULL
                          CHECK (event_type IN ('login','view','add_to_cart','checkout','purchase','error','logout')),
    prev_event_type   TEXT,
    prev_event_gap_s  DOUBLE PRECISION,                  -- velocity: gap < 0.01s → бот
    duration_ms       INTEGER,
    events_in_session INTEGER     NOT NULL DEFAULT 1,
    ip_address        TEXT,
    ip_events_last_1h INTEGER     NOT NULL DEFAULT 0,
    server_id         TEXT        NOT NULL DEFAULT 'server-1'
                          CHECK (server_id IN ('server-1','server-2','server-3')),  -- drift (#10)
    device_type       TEXT        NOT NULL DEFAULT 'desktop'
                          CHECK (device_type IN ('mobile','desktop','tablet')),
    is_bot_suspected  BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_events_user_id    ON events (user_id);
CREATE INDEX IF NOT EXISTS idx_events_session_id ON events (session_id);
CREATE INDEX IF NOT EXISTS idx_events_created_at ON events (created_at);
CREATE INDEX IF NOT EXISTS idx_events_server_id  ON events (server_id, created_at);
