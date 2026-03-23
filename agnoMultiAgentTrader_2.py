import os
import sys
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from ta import add_all_ta_features
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm


from agno.agent import Agent
from agno.team import Team
from agno.models.ollama import Ollama

# --- CONFIGURATION ---
DB_URL = 'postgresql://postgres:sarina00@127.0.0.1:5431/sp1500_1d'

# Connection pool for better performance
ENGINE = create_engine(
    DB_URL,
    poolclass=QueuePool,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# --- TOOLS ---

def _worker_ta(ticker: str, requested_columns: List[str], cutoff_date: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Internal worker for multiprocessing TA calculations.
    Each worker creates its own database connection for thread safety.
    """
    # Validate inputs
    if not ticker or not isinstance(ticker, str):
        return None

    safe_ticker = ticker.lower().strip()

    # Create a new engine for this worker to avoid thread safety issues
    worker_engine = create_engine(DB_URL, poolclass=QueuePool, pool_size=1)

    try:
        # Filter by cutoff_date if provided
        if cutoff_date:
            df = pd.read_sql(
                f'SELECT * FROM {safe_ticker} WHERE "Date" <= :cutoff_date ORDER BY "Date" DESC LIMIT 250',
                worker_engine, params={"cutoff_date": cutoff_date}
            )
        else:
            df = pd.read_sql(f'SELECT * FROM {safe_ticker} ORDER BY "Date" DESC LIMIT 250', worker_engine)

        if df.empty or len(df) < 50:
            return None

        df = df.sort_values(by="Date").reset_index(drop=True)
        df = add_all_ta_features(df, "Open", "High", "Low", "Close", "Volume", fillna=True)

        # Get the last available date (at or before cutoff_date)
        latest = df.iloc[-1]
        actual_date = latest['Date']

        res: Dict[str, Any] = {'ticker': ticker.upper(), 'close': round(latest['Close'], 2), 'data_date': actual_date}
        for col in requested_columns:
            if col in latest and pd.notna(latest[col]):
                res[col] = round(latest[col], 4)
        return res
    except Exception as e:
        logger.debug(f"Error processing {ticker}: {e}")
        return None
    finally:
        worker_engine.dispose()


def _worker_ta_wrapper(args_tuple):
    """Module-level wrapper for multiprocessing - unpacks args tuple."""
    return _worker_ta(*args_tuple)


def technical_screener(requested_indicators: List[str], sort_by: str = "ticker", cutoff_date: Optional[str] = None) -> str:
    """
    Screens the S&P 1500 using parallel processing.
    If cutoff_date is provided, only uses data up to that date.
    """
    with ENGINE.connect() as conn:
        res = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"))
        tickers = [row[0] for row in res if row[0] not in ['stock_metadata', 'stock_financials_quarterly', 'stock_financials_yearly']]

    cutoff_info = f" (as of {cutoff_date})" if cutoff_date else ""
    logger.info(f"🧬 Tech Analyst: Scanning {len(tickers)} stocks...{cutoff_info}")

    # Prepare args with cutoff_date
    args = [(ticker, requested_indicators, cutoff_date) for ticker in tickers]

    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        results = list(tqdm(executor.map(_worker_ta_wrapper, args), total=len(tickers)))

    df = pd.DataFrame([r for r in results if r is not None])
    if not df.empty and sort_by in df.columns:
        df = df.sort_values(by=sort_by).head(50)
    return df.to_csv(index=False)

def query_fundamental_health(tickers: List[str], period: str = "quarterly", cutoff_date: Optional[str] = None) -> str:
    """
    Analyzes fundamental data for the provided tickers.
    If cutoff_date is provided, only uses reports available on or before that date.
    """
    table = "stock_financials_quarterly" if period.lower() == "quarterly" else "stock_financials_yearly"

    # Filter by cutoff_date if provided
    if cutoff_date:
        date_filter = f'AND report_date <= :cutoff_date'
    else:
        date_filter = ''

    query = text(f"""
        WITH Ranked AS (
            SELECT ticker, report_date, total_revenue, net_income,
            LAG(total_revenue) OVER (PARTITION BY ticker ORDER BY report_date ASC) as prev_rev
            FROM {table} WHERE ticker = ANY(:t) {date_filter}
        )
        SELECT * FROM Ranked ORDER BY ticker, report_date DESC
    """)
    try:
        params: dict = {"t": [t.upper() for t in tickers]}
        if cutoff_date:
            params["cutoff_date"] = cutoff_date
        df = pd.read_sql(query, ENGINE, params=params)
        if df.empty: return f"No {period} data found."
        summary = []
        for t in tickers:
            t_df = df[df['ticker'] == t.upper()]
            if len(t_df) < 2: continue
            curr, prev = t_df.iloc[0], t_df.iloc[1]
            growth = (curr['total_revenue'] - curr['prev_rev']) / curr['prev_rev'] if curr['prev_rev'] else 0
            summary.append({
                'ticker': t.upper(), 'period': period, 'revenue_growth': f"{growth:.2%}",
                'trend': "Improving" if curr['total_revenue'] > prev['total_revenue'] else "Declining"
            })
        return pd.DataFrame(summary).to_csv(index=False)
    except Exception as e:
        return f"Error: {str(e)}"

# NEW TOOL: Metadata tool for the Risk Manager
def query_metadata(tickers: List[str]) -> str:
    """Fetches Sector, Market Cap, and Beta for a list of tickers."""
    query = text("SELECT ticker, name, sector, market_cap, beta FROM stock_metadata WHERE ticker = ANY(:t)")
    try:
        df = pd.read_sql(query, ENGINE, params={"t": [t.upper() for t in tickers]})
        return df.to_csv(index=False) if not df.empty else "No metadata found."
    except Exception as e:
        return f"Metadata Error: {str(e)}"

# NEW TOOL: Historical Performance Tracker
def get_historical_performance(tickers: List[str], cutoff_date: str) -> str:
    """
    Calculates the performance of stocks from cutoff_date to today.
    Returns the price at cutoff, current price, and % change.
    """
    if not cutoff_date:
        return "No cutoff_date provided."

    # Validate cutoff_date format
    try:
        datetime.strptime(cutoff_date, "%Y-%m-%d")
    except ValueError:
        return f"Invalid cutoff_date format. Use YYYY-MM-DD."

    results = []
    for ticker in tickers:
        # Validate ticker
        if not ticker or not isinstance(ticker, str) or not ticker.isalnum():
            logger.warning(f"Skipping invalid ticker: {ticker}")
            continue

        try:
            ticker_lower = ticker.lower().strip()

            # Get price at cutoff_date (or nearest prior date)
            price_at_cutoff_query = text(f'''
                SELECT "Close", "Date" FROM {ticker_lower}
                WHERE "Date" <= :cutoff_date
                ORDER BY "Date" DESC LIMIT 1
            ''')
            cutoff_df = pd.read_sql(price_at_cutoff_query, ENGINE, params={"cutoff_date": cutoff_date})

            if cutoff_df.empty:
                continue

            price_at_cutoff = cutoff_df.iloc[0]['Close']
            cutoff_actual_date = cutoff_df.iloc[0]['Date']

            # Get latest price (most recent available)
            latest_query = text(f'SELECT "Close", "Date" FROM {ticker_lower} ORDER BY "Date" DESC LIMIT 1')
            latest_df = pd.read_sql(latest_query, ENGINE)

            if latest_df.empty:
                continue

            current_price = latest_df.iloc[0]['Close']
            latest_date = latest_df.iloc[0]['Date']

            # Calculate returns
            pct_change = ((current_price - price_at_cutoff) / price_at_cutoff) * 100

            results.append({
                'ticker': ticker.upper(),
                'cutoff_date': str(cutoff_actual_date)[:10],
                'price_at_cutoff': round(price_at_cutoff, 2),
                'latest_date': str(latest_date)[:10],
                'current_price': round(current_price, 2),
                'pct_change': round(pct_change, 2)
            })
        except Exception as e:
            logger.warning(f"Error processing {ticker}: {e}")
            continue

    if not results:
        return "No performance data available."

    return pd.DataFrame(results).to_csv(index=False)

# --- AGENT DEFINITIONS ---

tech_agent = Agent(
    name="Technical Specialist",
    role="Identify price-action setups.",
    model=Ollama(id="minimax-m2.5:cloud"),
    tools=[technical_screener],
    instructions=["Return only the top 10-15 tickers that meet the criteria."]
)

fund_agent = Agent(
    name="Fundamental Specialist",
    role="Vet stocks for financial health.",
    model=Ollama(id="minimax-m2.5:cloud"),
    tools=[query_fundamental_health],
    instructions=["Check trends and reject weak companies."]
)

risk_manager = Agent(
    name="Risk Manager",
    role="Evaluate volatility and stability using metadata.",
    model=Ollama(id="minimax-m2.5:cloud"),
    tools=[query_metadata], # GAVE HIM EYES!
    instructions=[
        "Use 'query_metadata' to check Market Cap and Beta for the tickers.",
        "Flag 'Small Cap' (< 2B) or 'High Volatility' (Beta > 1.5).",
        "Ensure the final selection is not overly concentrated in one sector."
    ]
)

# NEW: Performance Analyst for historical backtesting
perf_analyst = Agent(
    name="Performance Analyst",
    role="Track historical performance from cutoff date to today.",
    model=Ollama(id="minimax-m2.5:cloud"),
    tools=[get_historical_performance],
    instructions=[
        "Use 'get_historical_performance' to calculate how stocks performed from the cutoff_date to today.",
        "Report the price at cutoff, current price, and percentage change.",
        "This helps evaluate if the screening criteria would have picked winners."
    ]
)

quant_team = Team(
    name="Quant Strategy Team",
    members=[tech_agent, fund_agent, risk_manager, perf_analyst],
    model=Ollama(id="minimax-m2.5:cloud"),
    instructions=[
        "1. Ask the Technical Specialist to find candidates (pass cutoff_date parameter if provided in the prompt).",
        "2. Pass candidates to the Fundamental Specialist for a health check (pass cutoff_date if provided).",
        "3. Have the Risk Manager use 'query_metadata' on the final list.",
        "4. Have the Performance Analyst calculate historical performance from cutoff_date to today.",
        "5. Synthesize everything into a final Markdown table with Technical, Fundamental, Risk, and Performance columns.",
        "⚠️ CRITICAL: Complete the task in ONE cycle. If no stocks pass all filters, explain WHY instead of searching again."
    ],
    markdown=True,
    debug_mode=True
)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Stock Screener with Historical Backtesting")
    parser.add_argument("--cutoff-date", type=str, default=None,
                        help="Cutoff date for screening (YYYY-MM-DD). If not provided, uses current date.")
    parser.add_argument("--prompt", type=str,
                        default="Find me 5 Small or Mid Cap stocks in an uptrend with consistent yearly revenue growth.",
                        help="Screening criteria prompt")

    args = parser.parse_args()

    # Validate cutoff_date if provided
    if args.cutoff_date:
        try:
            datetime.strptime(args.cutoff_date, "%Y-%m-%d")
        except ValueError:
            logger.error("Invalid cutoff_date format. Use YYYY-MM-DD.")
            sys.exit(1)

    cutoff_info = f" on {args.cutoff_date}" if args.cutoff_date else ""
    prompt = f"{args.prompt} cutoff_date={args.cutoff_date}" if args.cutoff_date else args.prompt

    logger.info(f"\n🚀 Quant Team initiating{cutoff_info}: {args.prompt}\n")
    quant_team.print_response(prompt)

    # Cleanup connection pool on exit
    ENGINE.dispose()
    logger.info("Database connections closed.")
