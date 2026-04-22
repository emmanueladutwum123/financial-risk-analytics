-- ============================================================
-- Core analytics queries — all export-ready for Tableau
-- ============================================================

-- ── 1. Daily returns & 30/90-day rolling annualised volatility ──────────────
CREATE OR REPLACE VIEW vw_rolling_volatility AS
WITH daily_returns AS (
    SELECT
        sp.ticker,
        s.sector_name,
        sp.date,
        sp.close,
        sp.close / LAG(sp.close) OVER (PARTITION BY sp.ticker ORDER BY sp.date) - 1 AS daily_return
    FROM stock_prices sp
    JOIN sectors s USING (ticker)
)
SELECT
    ticker,
    sector_name,
    date,
    close,
    daily_return,
    AVG(daily_return) OVER (
        PARTITION BY ticker ORDER BY date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) * 252                                                              AS rolling_30d_return_ann,
    STDDEV(daily_return) OVER (
        PARTITION BY ticker ORDER BY date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) * SQRT(252)                                                        AS rolling_30d_vol_ann,
    STDDEV(daily_return) OVER (
        PARTITION BY ticker ORDER BY date
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ) * SQRT(252)                                                        AS rolling_90d_vol_ann
FROM daily_returns
WHERE daily_return IS NOT NULL;


-- ── 2. Maximum drawdown per sector per calendar year ────────────────────────
CREATE OR REPLACE VIEW vw_annual_drawdown AS
WITH prices AS (
    SELECT ticker, date, close, EXTRACT(YEAR FROM date) AS yr
    FROM stock_prices
),
running_peak AS (
    SELECT
        ticker, date, yr, close,
        MAX(close) OVER (PARTITION BY ticker, yr ORDER BY date
                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS peak
    FROM prices
),
drawdown AS (
    SELECT ticker, date, yr, (close - peak) / peak AS drawdown
    FROM running_peak
)
SELECT
    d.ticker,
    s.sector_name,
    d.yr::INT AS year,
    MIN(d.drawdown)  AS max_drawdown,
    MAX(d.drawdown)  AS max_recovery
FROM drawdown d
JOIN sectors s USING (ticker)
GROUP BY d.ticker, s.sector_name, d.yr;


-- ── 3. Sharpe ratio by sector (rolling 252-day, annualised, rf = FEDFUNDS) ──
CREATE OR REPLACE VIEW vw_rolling_sharpe AS
WITH rf AS (
    -- Convert annual fed funds rate to daily
    SELECT date, fed_funds_rate / 100.0 / 252 AS daily_rf
    FROM macro_indicators
    WHERE fed_funds_rate IS NOT NULL
),
daily_returns AS (
    SELECT
        sp.ticker,
        sp.date,
        sp.close / LAG(sp.close) OVER (PARTITION BY sp.ticker ORDER BY sp.date) - 1 AS daily_return
    FROM stock_prices sp
),
excess AS (
    SELECT
        dr.ticker,
        dr.date,
        dr.daily_return - COALESCE(rf.daily_rf, 0) AS excess_return
    FROM daily_returns dr
    LEFT JOIN rf ON dr.date = rf.date
    WHERE dr.daily_return IS NOT NULL
)
SELECT
    e.ticker,
    s.sector_name,
    e.date,
    AVG(e.excess_return) OVER w * 252 /
    NULLIF(STDDEV(e.excess_return) OVER w * SQRT(252), 0) AS rolling_sharpe_252d
FROM excess e
JOIN sectors s USING (ticker)
WINDOW w AS (PARTITION BY e.ticker ORDER BY e.date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW);


-- ── 4. Sector correlation matrix (most recent 252 trading days) ─────────────
-- Used to generate correlation heatmap in Python; this query extracts the data
WITH recent_returns AS (
    SELECT
        ticker,
        date,
        close / LAG(close) OVER (PARTITION BY ticker ORDER BY date) - 1 AS r
    FROM stock_prices
    WHERE date >= (SELECT MAX(date) - INTERVAL '365 days' FROM stock_prices)
),
pivoted AS (
    SELECT date,
        MAX(r) FILTER (WHERE ticker = 'SPY')  AS spy,
        MAX(r) FILTER (WHERE ticker = 'XLF')  AS xlf,
        MAX(r) FILTER (WHERE ticker = 'XLK')  AS xlk,
        MAX(r) FILTER (WHERE ticker = 'XLE')  AS xle,
        MAX(r) FILTER (WHERE ticker = 'XLV')  AS xlv,
        MAX(r) FILTER (WHERE ticker = 'XLI')  AS xli,
        MAX(r) FILTER (WHERE ticker = 'XLC')  AS xlc,
        MAX(r) FILTER (WHERE ticker = 'XLY')  AS xly,
        MAX(r) FILTER (WHERE ticker = 'XLP')  AS xlp,
        MAX(r) FILTER (WHERE ticker = 'XLB')  AS xlb,
        MAX(r) FILTER (WHERE ticker = 'XLRE') AS xlre,
        MAX(r) FILTER (WHERE ticker = 'XLU')  AS xlu
    FROM recent_returns
    WHERE r IS NOT NULL
    GROUP BY date
)
SELECT * FROM pivoted ORDER BY date;


-- ── 5. Macro regime: yield curve inversion periods ──────────────────────────
CREATE OR REPLACE VIEW vw_macro_regimes AS
SELECT
    date,
    fed_funds_rate,
    yield_curve_spread,
    vix,
    CASE
        WHEN yield_curve_spread < 0               THEN 'Inverted (Recession Signal)'
        WHEN yield_curve_spread BETWEEN 0 AND 0.5 THEN 'Flattening'
        ELSE                                           'Normal'
    END AS yield_curve_regime,
    CASE
        WHEN vix > 30 THEN 'High Stress'
        WHEN vix > 20 THEN 'Elevated'
        ELSE               'Low Volatility'
    END AS market_stress_regime
FROM macro_indicators
WHERE yield_curve_spread IS NOT NULL AND vix IS NOT NULL;
