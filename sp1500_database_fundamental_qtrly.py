import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, text
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import time

DB_URL = 'postgresql://postgres:sarina00@127.0.0.1:5431/sp1500_1d'
engine = create_engine(DB_URL)

def fetch_quarterly_history(ticker):
    """Fetches quarterly financials with protective sleep."""
    # Safety sleep: 0.4s to protect against 3x API calls per ticker
    time.sleep(0.5)

    try:
        t = yf.Ticker(ticker)
        # Fetching the 3 required statements
        inc = t.quarterly_income_stmt.transpose()
        bal = t.quarterly_balance_sheet.transpose()
        cf = t.quarterly_cashflow.transpose()

        if inc.empty and bal.empty and cf.empty:
            return None

        # Merge and clean
        df = pd.concat([inc, bal, cf], axis=1)
        df = df.loc[:,~df.columns.duplicated()].copy()

        df['ticker'] = ticker.upper()
        df['report_date'] = df.index.astype(str)
        return df
    except Exception:
        return None

def build_quarterly_fundamentals():
    # 1. Get the list of all tickers from your price tables
    with engine.connect() as conn:
        query = text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        all_tickers = [row[0].upper() for row in conn.execute(query)
                       if row[0] not in ['stock_metadata', 'stock_financials_quarterly']]

    # 2. SMART RESUME: Check which tickers already have data
    existing_tickers = []
    try:
        with engine.connect() as conn:
            res = conn.execute(text("SELECT DISTINCT ticker FROM stock_financials_quarterly"))
            existing_tickers = [row[0].upper() for row in res]
        print(f"📂 Found {len(existing_tickers)} stocks already in Fundamentals. Resuming...")
    except Exception:
        print("📂 No existing fundamentals table found. Starting fresh.")

    # 3. Filter for missing tickers
    tickers_to_process = [t for t in all_tickers if t not in existing_tickers]

    if not tickers_to_process:
        print("✅ Quarterly fundamentals are already up to date!")
        return

    print(f"📊 Processing {len(tickers_to_process)} remaining stocks...")

    with ThreadPoolExecutor(max_workers=3) as executor: # Fewer workers = more safety
        results = list(tqdm(executor.map(fetch_quarterly_history, tickers_to_process), total=len(tickers_to_process)))

    # 4. Filter and Save
    valid_results = [r for r in results if r is not None and not r.empty]

    if valid_results:
        final_df = pd.concat(valid_results)
        final_df.columns = [c.lower().replace(" ", "_").replace("/", "_").replace("-", "_") for c in final_df.columns]

        # Use 'append' for Smart Resume
        final_df.to_sql('stock_financials_quarterly', engine, if_exists='append', index=False)

        # 5. Indexing for the Agent's speed
        with engine.connect() as conn:
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
            conn.execute(text('CREATE INDEX IF NOT EXISTS idx_ticker_date ON stock_financials_quarterly (ticker, report_date);'))

        print(f"✅ Added quarterly data for {len(valid_results)} stocks.")
    else:
        print("⚠️ No new data was retrieved.")

if __name__ == "__main__":
    build_quarterly_fundamentals()
