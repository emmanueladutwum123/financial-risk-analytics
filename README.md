# Financial Market Risk Analytics

A quantitative risk analytics system covering S&P 500 sector ETFs and macroeconomic regimes. Demonstrates end-to-end analytics: data engineering, advanced SQL, statistical modelling, and interactive visualisation.

**[Live Tableau Dashboard →](https://public.tableau.com/app/profile/emmanuel.adutwum)**

---

## Key Questions Answered

1. Which sectors carry the highest tail risk, and how has that changed over market regimes?
2. What is the probability that an equal-weight sector portfolio loses more than 10% over the next 30 trading days?
3. How correlated are sector returns during high-stress vs. low-stress periods (VIX-conditioned)?
4. Which sectors consistently produce risk-adjusted alpha (Sharpe > 1) across the full rate cycle?

---

## Methodology

### Data Sources
| Source | Content | Frequency |
|--------|---------|-----------|
| Yahoo Finance (via `yfinance`) | OHLCV prices for 11 sector ETFs + SPY benchmark | Daily |
| FRED API | Fed Funds Rate, CPI, Unemployment, 10Y-2Y Spread, VIX | Monthly / Daily |

### Analytics Pipeline

```
Data Collection (Python) → PostgreSQL (8 tables) → SQL Views → Python Modelling → Tableau
```

### Risk Models

**Value at Risk (VaR)**
- Historical simulation VaR at 95% and 99% confidence
- Parametric VaR with Cornish-Fisher expansion (corrects for non-normality / fat tails)
- CVaR / Expected Shortfall: mean loss beyond the VaR threshold

**Portfolio Performance**
- Sharpe Ratio (excess return / total volatility)
- Sortino Ratio (excess return / downside deviation only)
- Calmar Ratio (annualised return / maximum drawdown)
- Rolling 30-day and 90-day annualised volatility via SQL window functions

**Monte Carlo Simulation**
- 10,000 Geometric Brownian Motion paths over a 30-day horizon
- Equal-weight portfolio of 10 sector ETFs
- Outputs: terminal value distribution, P(loss), VaR at 95%/99%

**Macro Regime Analysis**
- Yield curve regime classification: Inverted / Flattening / Normal
- Market stress classification by VIX level
- Sector performance conditioned on regime

---

## SQL Highlights

All analytical queries use advanced PostgreSQL features:

| Technique | Query |
|-----------|-------|
| `LAG()` window function | Daily return calculation |
| `STDDEV() OVER (ROWS BETWEEN ...)` | Rolling volatility |
| `MAX() OVER (UNBOUNDED PRECEDING)` | Running peak for drawdown |
| `NTILE(5)` | Percentile ranking |
| Conditional aggregation (`FILTER`) | Pivot correlation data |
| CTEs | Multi-step metric composition |

---

## Project Structure

```
├── python/
│   ├── 01_data_collection.py     # Fetch prices + macro from APIs
│   └── 02_risk_analytics.py      # VaR, Sharpe, Monte Carlo, plots
├── sql/
│   ├── 01_schema.sql             # PostgreSQL table definitions
│   ├── 02_load_data.sql          # Bulk load CSVs via \copy
│   └── 03_analytics.sql          # 5 analytical views
├── tableau-exports/              # CSVs consumed by Tableau
├── docs/
│   ├── correlation_heatmap.png
│   └── monte_carlo.png
└── requirements.txt
```

---

## Setup

```bash
# 1. Create PostgreSQL database
createdb financial_risk

# 2. Install dependencies
pip install -r requirements.txt

# 3. Collect data
python python/01_data_collection.py

# 4. Load schema and data
psql -d financial_risk -f sql/01_schema.sql
psql -d financial_risk -f sql/02_load_data.sql

# 5. Create analytical views
psql -d financial_risk -f sql/03_analytics.sql

# 6. Run risk analytics
export DATABASE_URL=postgresql://localhost/financial_risk
python python/02_risk_analytics.py
```

---

## Selected Findings

- **XLE (Energy)** carried the highest 99% VaR across the 2020 stress period, exceeding 8% single-day loss potential
- The equal-weight portfolio showed P(loss > 10% in 30 days) ≈ 12% under current volatility conditions
- **Yield curve inversions** (2019, 2022–2023) preceded drawdowns in XLF and XLRE by 6–9 months
- **XLK (Technology)** produced the highest Sharpe ratio (1.42) over the full period but the worst Sortino during rate-rising regimes

---

## Tools & Technologies

`PostgreSQL` · `Python` · `pandas` · `NumPy` · `SciPy` · `Matplotlib` · `Seaborn` · `yfinance` · `Tableau Public`
