"""
Risk analytics: VaR (historical + parametric + CVaR), Monte Carlo portfolio
simulation, Sharpe/Sortino/Calmar ratios, correlation heatmap.
Exports CSVs and PNGs to ../tableau-exports/ and ../docs/.
"""

import os
import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from scipy import stats
from sqlalchemy import create_engine

warnings.filterwarnings("ignore")

DB_URL = os.getenv("DATABASE_URL", "postgresql://localhost/financial_risk")
OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "tableau-exports")
DOCS_DIR = os.path.join(os.path.dirname(__file__), "..", "docs")
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(DOCS_DIR, exist_ok=True)

CONFIDENCE_LEVELS = [0.95, 0.99]
PORTFOLIO_WEIGHTS = None   # None = equal-weight; set dict for custom
TRADING_DAYS = 252
N_SIMULATIONS = 10_000
HORIZON_DAYS = 30


# ── Helpers ──────────────────────────────────────────────────────────────────

def load_returns(engine) -> pd.DataFrame:
    q = """
        SELECT ticker, date,
               close / LAG(close) OVER (PARTITION BY ticker ORDER BY date) - 1 AS ret
        FROM stock_prices
        ORDER BY ticker, date
    """
    df = pd.read_sql(q, engine).dropna()
    return df.pivot(index="date", columns="ticker", values="ret")


def load_rolling_metrics(engine) -> pd.DataFrame:
    q = "SELECT * FROM vw_rolling_volatility ORDER BY ticker, date"
    return pd.read_sql(q, engine)


# ── Value at Risk ─────────────────────────────────────────────────────────────

def compute_var(returns: pd.Series, confidence: float = 0.95) -> dict:
    """Historical, Parametric (Normal), and CVaR for a single return series."""
    alpha = 1 - confidence

    # Historical
    var_hist = -np.percentile(returns.dropna(), alpha * 100)

    # Parametric (Cornish-Fisher expansion for non-normality correction)
    mu, sigma = returns.mean(), returns.std()
    z = stats.norm.ppf(alpha)
    s = stats.skew(returns.dropna())
    k = stats.kurtosis(returns.dropna())
    z_cf = (z + (z**2 - 1) * s / 6
              + (z**3 - 3*z) * k / 24
              - (2*z**3 - 5*z) * s**2 / 36)
    var_param = -(mu + z_cf * sigma)

    # CVaR (Expected Shortfall)
    cvar = -returns[returns <= -var_hist].mean()

    return {
        "confidence": confidence,
        "var_historical": round(var_hist, 6),
        "var_parametric_cf": round(var_param, 6),
        "cvar_es": round(cvar, 6),
    }


def var_report(returns: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for ticker in returns.columns:
        for cl in CONFIDENCE_LEVELS:
            row = compute_var(returns[ticker], cl)
            row["ticker"] = ticker
            rows.append(row)
    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(OUT_DIR, "var_report.csv"), index=False)
    print(f"VaR report: {len(df)} rows")
    return df


# ── Portfolio performance metrics ─────────────────────────────────────────────

def portfolio_metrics(returns: pd.DataFrame, rf_annual: float = 0.05) -> pd.DataFrame:
    rf_daily = rf_annual / TRADING_DAYS
    rows = []
    for ticker in returns.columns:
        r = returns[ticker].dropna()
        excess = r - rf_daily
        total_return = (1 + r).prod() - 1
        ann_return = (1 + total_return) ** (TRADING_DAYS / len(r)) - 1
        ann_vol = r.std() * np.sqrt(TRADING_DAYS)
        sharpe = excess.mean() / excess.std() * np.sqrt(TRADING_DAYS)

        # Sortino (downside deviation only)
        downside = r[r < 0].std() * np.sqrt(TRADING_DAYS)
        sortino = (ann_return - rf_annual) / downside if downside > 0 else np.nan

        # Calmar (return / max drawdown)
        cum = (1 + r).cumprod()
        peak = cum.cummax()
        drawdown = (cum - peak) / peak
        max_dd = drawdown.min()
        calmar = ann_return / abs(max_dd) if max_dd < 0 else np.nan

        # Skewness & excess kurtosis (normality)
        skew = stats.skew(r)
        kurt = stats.kurtosis(r)

        rows.append({
            "ticker": ticker,
            "ann_return": round(ann_return, 6),
            "ann_volatility": round(ann_vol, 6),
            "sharpe_ratio": round(sharpe, 4),
            "sortino_ratio": round(sortino, 4),
            "calmar_ratio": round(calmar, 4),
            "max_drawdown": round(max_dd, 6),
            "skewness": round(skew, 4),
            "excess_kurtosis": round(kurt, 4),
        })

    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(OUT_DIR, "portfolio_metrics.csv"), index=False)
    print(f"Portfolio metrics saved.")
    return df


# ── Monte Carlo simulation ─────────────────────────────────────────────────────

def monte_carlo_simulation(returns: pd.DataFrame) -> pd.DataFrame:
    """
    GBM simulation of an equal-weight portfolio over HORIZON_DAYS.
    Returns distribution of terminal portfolio values.
    """
    tickers = [t for t in returns.columns if t != "SPY"]
    n = len(tickers)
    weights = np.ones(n) / n

    port_returns = returns[tickers].dropna().dot(weights)
    mu = port_returns.mean()
    sigma = port_returns.std()

    # Geometric Brownian Motion
    rng = np.random.default_rng(42)
    dt = 1
    shocks = rng.normal(mu * dt, sigma * np.sqrt(dt), size=(N_SIMULATIONS, HORIZON_DAYS))
    paths = np.cumprod(1 + shocks, axis=1)

    terminal = paths[:, -1]
    results = pd.DataFrame({
        "simulation_id": range(N_SIMULATIONS),
        "terminal_value": terminal,
        "pnl_pct": terminal - 1,
    })

    # Summary stats
    summary = {
        "mean_terminal": terminal.mean(),
        "median_terminal": np.median(terminal),
        "var_95": np.percentile(terminal, 5),
        "var_99": np.percentile(terminal, 1),
        "prob_loss": (terminal < 1).mean(),
    }

    results.to_csv(os.path.join(OUT_DIR, "monte_carlo_paths.csv"), index=False)
    pd.DataFrame([summary]).to_csv(os.path.join(OUT_DIR, "monte_carlo_summary.csv"), index=False)
    print(f"Monte Carlo: P(loss)={summary['prob_loss']:.2%}, VaR95={summary['var_95']:.4f}")
    return results, summary


# ── Visualisations ────────────────────────────────────────────────────────────

def plot_correlation_heatmap(returns: pd.DataFrame):
    corr = returns.corr()
    fig, ax = plt.subplots(figsize=(12, 10))
    mask = np.triu(np.ones_like(corr, dtype=bool))
    sns.heatmap(corr, mask=mask, annot=True, fmt=".2f", cmap="RdYlGn",
                center=0, linewidths=0.5, ax=ax, vmin=-1, vmax=1)
    ax.set_title("Sector ETF Return Correlation Matrix (Full History)", fontsize=14, pad=15)
    plt.tight_layout()
    fig.savefig(os.path.join(DOCS_DIR, "correlation_heatmap.png"), dpi=150)
    plt.close()
    corr.to_csv(os.path.join(OUT_DIR, "correlation_matrix.csv"))
    print("Correlation heatmap saved.")


def plot_monte_carlo(results: pd.DataFrame, summary: dict):
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # Distribution of terminal values
    axes[0].hist(results["pnl_pct"] * 100, bins=80, color="#2196F3", alpha=0.7, edgecolor="white")
    axes[0].axvline((summary["var_95"] - 1) * 100, color="red", linestyle="--",
                    label=f"VaR 95%: {(summary['var_95']-1)*100:.1f}%")
    axes[0].axvline((summary["var_99"] - 1) * 100, color="darkred", linestyle="--",
                    label=f"VaR 99%: {(summary['var_99']-1)*100:.1f}%")
    axes[0].set_xlabel("30-Day Portfolio Return (%)")
    axes[0].set_ylabel("Frequency")
    axes[0].set_title(f"Monte Carlo: {N_SIMULATIONS:,} Simulations")
    axes[0].legend()

    # Sample paths (first 200)
    sample = results["terminal_value"].values[:200]
    rng = np.random.default_rng(42)
    dt = 1
    mu = results["terminal_value"].mean() ** (1 / HORIZON_DAYS) - 1
    sigma_est = results["pnl_pct"].std() / np.sqrt(HORIZON_DAYS)
    for _ in range(200):
        path = np.cumprod(1 + rng.normal(mu, sigma_est, HORIZON_DAYS))
        axes[1].plot(path, alpha=0.08, color="#1565C0", linewidth=0.5)
    axes[1].axhline(1.0, color="black", linewidth=1)
    axes[1].set_xlabel("Trading Days")
    axes[1].set_ylabel("Portfolio Value (Base = 1.0)")
    axes[1].set_title("Sample GBM Paths (200 of 10,000)")

    plt.tight_layout()
    fig.savefig(os.path.join(DOCS_DIR, "monte_carlo.png"), dpi=150)
    plt.close()
    print("Monte Carlo plot saved.")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    engine = create_engine(DB_URL)
    returns = load_returns(engine)
    print(f"Loaded returns: {returns.shape} ({returns.index.min()} → {returns.index.max()})")

    var_report(returns)
    metrics = portfolio_metrics(returns)
    print(metrics[["ticker", "sharpe_ratio", "sortino_ratio", "max_drawdown"]].to_string(index=False))

    mc_results, mc_summary = monte_carlo_simulation(returns)

    plot_correlation_heatmap(returns)
    plot_monte_carlo(mc_results, mc_summary)

    print("\nAll outputs written to tableau-exports/ and docs/")
