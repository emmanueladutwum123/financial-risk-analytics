-- ============================================================
-- Load CSVs into PostgreSQL using \copy (psql client-side)
-- Run from the project root: psql -d financial_risk -f sql/02_load_data.sql
-- ============================================================

\copy sectors         FROM 'data/sectors.csv'          CSV HEADER;
\copy stock_prices (ticker, date, close, open, high, low, volume) FROM 'data/stock_prices.csv' CSV HEADER;
\copy macro_indicators FROM 'data/macro_indicators.csv' CSV HEADER;

SELECT 'sectors'         AS table_name, COUNT(*) FROM sectors
UNION ALL
SELECT 'stock_prices',   COUNT(*) FROM stock_prices
UNION ALL
SELECT 'macro_indicators', COUNT(*) FROM macro_indicators;
