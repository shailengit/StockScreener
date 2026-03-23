import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, text
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import time

DB_URL = 'postgresql://postgres:sarina00@127.0.0.1:5431/sp1500_1d'
engine = create_engine(DB_URL)

def fetch_yearly_history(ticker):
    """Fetches yearly financials (last 4 years) with protective sleep."""
    time.sleep(0.5)
    try:
        t = yf.Ticker(ticker)
        # Yearly versions of the statements
        inc = t.income_stmt.transpose()
        bal = t.balance_sheet.transpose()
        cf = t.cashflow.transpose()

        if inc.empty and bal.empty and cf.empty:
            return None

        df = pd.concat([inc, bal, cf], axis=1)
        df = df.loc[:,~df.columns.duplicated()].copy()
        df['ticker'] = ticker.upper()
        df['report_date'] = df.index.astype(str)
        return df
    except Exception:
        return None

def build_yearly_fundamentals():
    with engine.connect() as conn:
        query = text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        all_tickers = [row[0].upper() for row in conn.execute(query)
                       if row[0] not in ['stock_metadata', 'stock_financials_quarterly', 'stock_financials_yearly']]

    # SMART RESUME
    existing_tickers = []
    try:
        with engine.connect() as conn:
            res = conn.execute(text("SELECT DISTINCT ticker FROM stock_financials_yearly"))
            existing_tickers = [row[0].upper() for row in res]
        print(f"📂 Found {len(existing_tickers)} stocks already in Yearly data. Resuming...")
    except Exception:
        print("📂 No existing yearly table found. Starting fresh.")

    tickers_to_process = [t for t in all_tickers if t not in existing_tickers]
    if not tickers_to_process:
        print("✅ Yearly fundamentals are up to date!")
        return

    print(f"📊 Processing {len(tickers_to_process)} remaining stocks (Yearly)...")
    with ThreadPoolExecutor(max_workers=3) as executor:
        results = list(tqdm(executor.map(fetch_yearly_history, tickers_to_process), total=len(tickers_to_process)))

    valid_results = [r for r in results if r is not None and not r.empty]
    if valid_results:
        final_df = pd.concat(valid_results)
        final_df.columns = [c.lower().replace(" ", "_").replace("/", "_").replace("-", "_") for c in final_df.columns]
        final_df.to_sql('stock_financials_yearly', engine, if_exists='append', index=False)

        with engine.connect() as conn:
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
            conn.execute(text('CREATE INDEX IF NOT EXISTS idx_ticker_date_yr ON stock_financials_yearly (ticker, report_date);'))
        print(f"✅ Yearly data for {len(valid_results)} stocks added.")

if __name__ == "__main__":
    build_yearly_fundamentals()
