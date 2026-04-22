-- ============================================================
-- Project 1: Financial Risk Analytics
-- Schema: stock prices, sectors, macro indicators
-- ============================================================

CREATE TABLE IF NOT EXISTS sectors (
    ticker       VARCHAR(10) PRIMARY KEY,
    sector_name  VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS stock_prices (
    id          SERIAL PRIMARY KEY,
    ticker      VARCHAR(10)    NOT NULL REFERENCES sectors(ticker),
    date        DATE           NOT NULL,
    open        NUMERIC(12, 4),
    high        NUMERIC(12, 4),
    low         NUMERIC(12, 4),
    close       NUMERIC(12, 4) NOT NULL,
    volume      BIGINT,
    UNIQUE (ticker, date)
);

CREATE TABLE IF NOT EXISTS macro_indicators (
    date                DATE PRIMARY KEY,
    fed_funds_rate      NUMERIC(8, 4),
    cpi                 NUMERIC(10, 4),
    unemployment_rate   NUMERIC(6, 3),
    yield_curve_spread  NUMERIC(8, 4),
    vix                 NUMERIC(8, 4)
);

-- Indexes for analytics query performance
CREATE INDEX idx_stock_prices_ticker ON stock_prices(ticker);
CREATE INDEX idx_stock_prices_date   ON stock_prices(date);
CREATE INDEX idx_macro_date          ON macro_indicators(date);
