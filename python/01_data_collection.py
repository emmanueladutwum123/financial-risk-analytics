"""
Data collection: S&P 500 sector ETFs + macroeconomic indicators from FRED.
Saves raw CSVs to ../data/ for loading into PostgreSQL.
"""

import yfinance as yf
import pandas as pd
import requests
import os
from datetime import datetime

# S&P 500 sector ETFs
SECTOR_TICKERS = {
    "SPY": "S&P 500 (Benchmark)",
    "XLF": "Financials",
    "XLK": "Technology",
    "XLE": "Energy",
    "XLV": "Health Care",
    "XLI": "Industrials",
    "XLC": "Communication Services",
    "XLY": "Consumer Discretionary",
    "XLP": "Consumer Staples",
    "XLB": "Materials",
    "XLRE": "Real Estate",
    "XLU": "Utilities",
}

START_DATE = "2015-01-01"
END_DATE = datetime.today().strftime("%Y-%m-%d")
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def fetch_price_data():
    tickers = list(SECTOR_TICKERS.keys())
    raw = yf.download(tickers, start=START_DATE, end=END_DATE, auto_adjust=True)

    # Reshape to long format
    close = raw["Close"].reset_index().melt(id_vars="Date", var_name="ticker", value_name="close")
    open_ = raw["Open"].reset_index().melt(id_vars="Date", var_name="ticker", value_name="open")
    high = raw["High"].reset_index().melt(id_vars="Date", var_name="ticker", value_name="high")
    low = raw["Low"].reset_index().melt(id_vars="Date", var_name="ticker", value_name="low")
    volume = raw["Volume"].reset_index().melt(id_vars="Date", var_name="ticker", value_name="volume")

    df = close.merge(open_, on=["Date", "ticker"])
    df = df.merge(high, on=["Date", "ticker"])
    df = df.merge(low, on=["Date", "ticker"])
    df = df.merge(volume, on=["Date", "ticker"])
    df.rename(columns={"Date": "date"}, inplace=True)
    df.dropna(inplace=True)

    out = os.path.join(DATA_DIR, "stock_prices.csv")
    df.to_csv(out, index=False)
    print(f"Saved {len(df)} rows to {out}")
    return df


def fetch_macro_data():
    """Fetch key macro indicators from FRED (no API key needed for basic series)."""
    # Using FRED public CSV endpoints
    series = {
        "FEDFUNDS": "fed_funds_rate",
        "CPIAUCSL": "cpi",
        "UNRATE": "unemployment_rate",
        "T10Y2Y": "yield_curve_spread",
        "VIXCLS": "vix",
    }

    frames = []
    for series_id, col_name in series.items():
        url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
        try:
            df = pd.read_csv(url)
            df.columns = [c.strip() for c in df.columns]
            date_col = df.columns[0]
            val_col  = df.columns[1]
            df.rename(columns={date_col: "date", val_col: col_name}, inplace=True)
            df["date"] = pd.to_datetime(df["date"])
            df = df[df["date"] >= START_DATE]
            df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
            frames.append(df.set_index("date"))
            print(f"Fetched {series_id}")
        except Exception as e:
            print(f"Failed {series_id}: {e}")

    if frames:
        macro = pd.concat(frames, axis=1).reset_index()
        out = os.path.join(DATA_DIR, "macro_indicators.csv")
        macro.to_csv(out, index=False)
        print(f"Saved macro data to {out}")
        return macro


def build_sector_ref():
    df = pd.DataFrame([
        {"ticker": k, "sector_name": v} for k, v in SECTOR_TICKERS.items()
    ])
    out = os.path.join(DATA_DIR, "sectors.csv")
    df.to_csv(out, index=False)
    print(f"Saved sector reference to {out}")


if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)
    fetch_price_data()
    fetch_macro_data()
    build_sector_ref()
    print("Data collection complete.")
