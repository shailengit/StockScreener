import pandas as pd
import yfinance as yf
import re
from tqdm import tqdm
from sqlalchemy import create_engine, text
from datetime import datetime
from time import sleep

def clean_symbols(string):
    """Cleans symbols like BRK.B to BRK-B for yfinance compatibility."""
    return string.replace('.', '-')

class SP1500Database:
    def __init__(self, interval='1d'):
        self.interval = interval
        self.user = "postgres"
        self.password = "sarina00"
        self.host = "127.0.0.1"
        self.port = "5431"
        self.db_name = f"sp1500_{self.interval}".lower()
        self.tickers = []
        self.engine = None
        # ETF List - Sector ETFs to track
        self.ETF_LIST = ['XLK', 'XLV', 'XLF', 'XLY', 'XLI', 'XLC', 'XLP', 'XLE', 'XLB', 'XLRE', 'XLU']
        # Initialize engine early to allow ticker fetching from DB
        conn_str = f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}'
        self.engine = create_engine(conn_str)

    def fetch_sp1500_tickers(self):
            """
            Gets tickers from the database first.
            Falls back to Wikipedia only if the database is empty.
            """
            print(f"🔍 Checking database '{self.db_name}' for existing tickers...")

            # Ensure engine is initialized before attempting to connect
            if self.engine is None:
                conn_str = f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}'
                self.engine = create_engine(conn_str)

            try:
                with self.engine.connect() as conn:
                    # Query to get all user-created table names
                    query = text("""
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name NOT IN ('stock_metadata', 'stock_financials_quarterly')
                    """)
                    res = conn.execute(query)
                    db_tickers = [row[0].upper() for row in res]

                if db_tickers:
                    self.tickers = sorted(db_tickers)
                    # Always add ETFs - they may not be in the DB yet
                    self.tickers.extend(self.ETF_LIST)
                    self.tickers = sorted(set(self.tickers))
                    print(f"✅ Found {len(self.tickers) - len(self.ETF_LIST)} tickers in local database + {len(self.ETF_LIST)} ETFs")
                    return # Exit early since we have our list

            except Exception as e:
                print(f"ℹ️ Database not yet created or accessible: {e}")

            # --- FALLBACK TO WIKIPEDIA ---
            print("📥 Database empty. Fetching S&P 1500 Ticker lists from Wikipedia...")
            urls = {
                "S&P 500": "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
                "S&P 400": "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies",
                "S&P 600": "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies"
            }

            all_symbols = []
            headers = {"User-Agent": "Mozilla/5.0"}

            for name, url in urls.items():
                try:
                    # Note: 'lxml' is required here. Run 'pip install lxml' in your terminal.
                    df_list = pd.read_html(url, storage_options=headers)
                    df = df_list[0]
                    col = 'Symbol' if 'Symbol' in df.columns else 'Ticker'
                    symbols = df[col].astype(str).apply(clean_symbols).tolist()
                    all_symbols.extend(symbols)
                    print(f"✅ Found {len(symbols)} in {name}")
                except Exception as e:
                    print(f"⚠️ Wikipedia fetch failed for {name}: {e}")

            self.tickers = sorted(list(set(all_symbols)))
            print(f"🚀 Total unique tickers to process: {len(self.tickers)}")

            # --- ADD ETFs TO THE LIST ---
            self.tickers.extend(self.ETF_LIST)
            self.tickers = sorted(set(self.tickers))
            print(f"📈 Added {len(self.ETF_LIST)} ETFs to the list. Total: {len(self.tickers)} tickers")

    def setup_postgres(self):
        """Handles database creation and connectivity."""
        main_url = f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/postgres'
        main_engine = create_engine(main_url)

        with main_engine.connect() as conn:
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
            check_db = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :dbname"),
                {"dbname": self.db_name}
            ).fetchone()

            if not check_db:
                print(f"🛠️ Creating database '{self.db_name}'...")
                conn.execute(text(f'CREATE DATABASE "{self.db_name}"'))

    def run_update(self):
        """Processes each ticker: creates table or appends new data."""
        if self.engine is None:
            raise ValueError("Engine not initialized.")

        if not self.tickers:
            print("❌ No tickers found to update. Check your database or internet connection.")
            return

        # Calculate trading day cutoff: skip API calls if last data is today or yesterday
        # Markets are closed on weekends, so no new data is available
        from datetime import timedelta
        today = datetime.now().date()
        trading_day_cutoff = today - timedelta(days=1)

        print(f"🛠️ Updating database '{self.db_name}'...")
        print(f"📅 Today: {today} | Skipping API call if last stored date >= {trading_day_cutoff}")

        for symbol in tqdm(self.tickers):
            table_name = symbol.lower()

            try:
                col = "Datetime" if self.interval == '1m' else "Date"

                with self.engine.connect() as conn:
                    query = text(f'SELECT MAX("{col}") FROM {table_name}')
                    result = conn.execute(query).fetchone()
                    max_date = result[0] if result else None

                if max_date is None:
                    raise ValueError("Empty")

                start_date = pd.to_datetime(max_date)

                if getattr(start_date, 'tzinfo', None) is not None:
                    start_date = start_date.replace(tzinfo=None)

                # Check against trading_day_cutoff instead of today to avoid unnecessary API calls
                if start_date.date() < trading_day_cutoff:
                    sleep(1.0)  # Only sleep when actually making an API call
                    print(f"\n📥 [{symbol}] Last date: {start_date.date()} — Fetching new data from {start_date.date()}...")
                    new_data = yf.Ticker(symbol).history(interval=self.interval, start=start_date)

                    if not new_data.empty:
                        new_data.index = pd.to_datetime(new_data.index)
                        if getattr(new_data.index, 'tz', None) is not None:
                            new_data.index = new_data.index.tz_localize(None) # type: ignore

                        new_rows = new_data[new_data.index > start_date]
                        if not new_rows.empty:
                            print(f"   ✅ [{symbol}] Appending {len(new_rows)} new rows")
                            new_rows.to_sql(table_name, self.engine, if_exists='append', method='multi', chunksize=5000)
                        else:
                            print(f"   ℹ️ [{symbol}] No new rows to append (data may already be up to date)")
                    else:
                        print(f"   ⚠️ [{symbol}] yfinance returned empty data")
                else:
                    print(f"   ⏭️ [{symbol}] Skipping — last date {start_date.date()} is within trading day cutoff")

            except Exception:
                # Initial Download Logic
                try:
                    sleep(1.0)  # Rate limit even for initial downloads
                    print(f"\n📥 [{symbol}] No existing data found — downloading full history...")
                    data = yf.Ticker(symbol).history(interval=self.interval, period='max')
                    if not data.empty:
                        data.index = pd.to_datetime(data.index)
                        if getattr(data.index, 'tz', None) is not None:
                            data.index = data.index.tz_localize(None) # type: ignore

                        data.to_sql(table_name, self.engine, if_exists='replace', method='multi', chunksize=5000)
                        print(f"   ✅ [{symbol}] Stored {len(data)} rows to new table")

                        with self.engine.connect() as conn:
                            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
                            idx_col = "Datetime" if self.interval == '1m' else "Date"
                            conn.execute(text(f'CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_date ON {table_name} ("{idx_col}");'))
                        print(f"   🔧 [{symbol}] Created index on {idx_col} column")
                except Exception:
                    print(f"   ❌ [{symbol}] Failed to download/store data")
                    continue

if __name__ == "__main__":
    db_manager = SP1500Database(interval='1d')
    # Order changed: Setup DB first so we can check it for tickers
    db_manager.setup_postgres()
    db_manager.fetch_sp1500_tickers()
    db_manager.run_update()
