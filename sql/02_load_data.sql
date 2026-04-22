-- ============================================================
-- Load CSVs into PostgreSQL using \copy (psql client-side)
-- Run from the project root: psql -d financial_risk -f sql/02_load_data.sql
-- ============================================================

\copy sectors         FROM 'data/sectors.csv'          CSV HEADER;
\copy macro_indicators FROM 'data/macro_indicators.csv' CSV HEADER;

-- Load stock prices via a staging table to handle float volume
CREATE TEMP TABLE stock_prices_stage (
    date DATE, ticker VARCHAR(10), close NUMERIC, open NUMERIC,
    high NUMERIC, low NUMERIC, volume NUMERIC
);
\copy stock_prices_stage FROM 'data/stock_prices.csv' CSV HEADER;
INSERT INTO stock_prices (date, ticker, close, open, high, low, volume)
SELECT date, ticker, close, open, high, low, volume::BIGINT FROM stock_prices_stage
ON CONFLICT (ticker, date) DO NOTHING;
DROP TABLE stock_prices_stage;

SELECT 'sectors'         AS table_name, COUNT(*) FROM sectors
UNION ALL
SELECT 'stock_prices',   COUNT(*) FROM stock_prices
UNION ALL
SELECT 'macro_indicators', COUNT(*) FROM macro_indicators;
