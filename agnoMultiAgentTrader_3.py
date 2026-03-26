import os
import sys
import logging
import pandas as pd
import numpy as np
import concurrent.futures
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from agno.agent import Agent
from agno.team import Team
from agno.models.ollama import Ollama

# --- 0. Model Configuration ---
# Configure the Ollama model ID to use for all agents
OLLAMA_MODEL_ID = "glm-5:cloud"  # Default model for primary agents
OLLAMA_MODEL_ID_FALLBACK = "glm-5:cloud"     # Fallback/secondary model (used for Fundamental Specialist)

# --- 1. Database Configuration & Helper Functions ---
DB_URL = 'postgresql://postgres:sarina00@127.0.0.1:5431/sp1500_1d'

ENGINE = create_engine(
    DB_URL,
    poolclass=QueuePool,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def get_active_tickers():
    with ENGINE.connect() as conn:
        res = conn.execute(text("SELECT ticker FROM stock_metadata WHERE ticker IS NOT NULL"))
        tickers = [row[0] for row in res]
    # Filter out sector ETFs and financial tables that share table names with tickers
    skip_tables = {'xlb', 'xlc', 'xle', 'xlf', 'xli', 'xlk', 'xlp', 'xlre', 'xlu', 'xlv', 'xly',
                   'stock_financials_quarterly', 'stock_financials_yearly', 'stock_metadata',
                   'all', 'aci', 'cns', 'brk-b', 'bf-b', 'on', 'v', 't', 'w', 'gs', 'd', 'n', 'ko', 'sn', 'zto', 'ac', 'nls', 'vod', 'wtv'}
    return [t for t in tickers if t.lower() not in skip_tables]

# --- 2. Tool Definitions for the Agents ---

def analyze_single_ticker_technical(ticker: str) -> dict | None:
    """Worker function for ProcessPoolExecutor to analyze technicals."""
    worker_engine = create_engine(DB_URL, poolclass=QueuePool, pool_size=1)
    try:
        query = f'SELECT "Date", "Close", "Volume", "High" FROM "{ticker.lower()}" ORDER BY "Date" DESC LIMIT 200;'
        df = pd.read_sql(query, worker_engine).sort_values('Date')
    except Exception:
        return None
    finally:
        worker_engine.dispose()

    if len(df) < 120:
        return None

    # Squeeze Logic
    df['sma'] = df['Close'].rolling(window=20).mean()
    df['std'] = df['Close'].rolling(window=20).std()
    df['bandwidth'] = ((df['sma'] + (df['std'] * 2)) - (df['sma'] - (df['std'] * 2))) / df['sma']

    is_squeezing = df['bandwidth'].iloc[-1] <= (df['bandwidth'].tail(120).min() * 1.15)

    # OBV Logic
    close_diff = df['Close'].diff()
    df['obv'] = pd.Series(np.sign(close_diff.values) * df['Volume'].values).fillna(0).cumsum()
    obv_slope = np.polyfit(np.arange(20), df['obv'].tail(20), 1)[0]
    price_slope = np.polyfit(np.arange(20), df['Close'].tail(20), 1)[0]
    hidden_accumulation = (obv_slope > 0) and (abs(price_slope) < (df['Close'].iloc[-1] * 0.005))

    # Breakout Logic
    past_resistance = df['High'].shift(3).rolling(window=120).max().iloc[-1]
    is_breakout = (df['Close'].iloc[-1] > past_resistance) and (df['Volume'].iloc[-1] > (df['Volume'].tail(50).mean() * 1.5))

    if is_breakout:
        return {"ticker": ticker.upper(), "signal": "Active Breakout"}
    elif is_squeezing and hidden_accumulation:
        return {"ticker": ticker.upper(), "signal": "Coiling (Accumulation)"}
    return None

def tool_run_technical_scan() -> list:
    """Tool for the Technical Specialist to run a parallelized database scan."""
    tickers = get_active_tickers()
    results = []

    # Using ProcessPoolExecutor for fast indicator calculation across PostgreSQL tables
    with concurrent.futures.ProcessPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(analyze_single_ticker_technical, t): t for t in tickers}
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                if result:
                    results.append(result)
            except Exception:
                pass

    return results

def tool_verify_fundamental_inflection(tickers: list) -> list:
    """Tool for the Fundamental Specialist to check EPS acceleration."""
    verified_tickers = []

    for item in tickers:
        ticker = item['ticker']
        try:
            query = text("""
                SELECT eps FROM stock_financials_quarterly
                WHERE ticker = :ticker ORDER BY report_date DESC LIMIT 3;
            """)
            with ENGINE.connect() as conn:
                fin_df = pd.read_sql(query, conn, params={"ticker": ticker})

            if len(fin_df) == 3:
                current_eps, prev_eps, older_eps = fin_df['eps'].iloc[0], fin_df['eps'].iloc[1], fin_df['eps'].iloc[2]

                if prev_eps != 0 and older_eps != 0:
                    current_growth = (current_eps - prev_eps) / abs(prev_eps)
                    prev_growth = (prev_eps - older_eps) / abs(older_eps)

                    if (current_growth > 0) and (current_growth > prev_growth * 1.5):
                        item['fundamental_catalyst'] = "Confirmed EPS Acceleration"
                        verified_tickers.append(item)
        except Exception:
            pass

    return verified_tickers

# --- 3. Agno Agent Initialization ---

tech_specialist = Agent(
    name="Technical Specialist",
    role="Identify stocks experiencing volatility contraction (Bollinger Squeeze), hidden institutional accumulation (OBV), or key resistance breakouts.",
    tools=[tool_run_technical_scan],
    model=Ollama(id=OLLAMA_MODEL_ID),
    instructions="Call the `tool_run_technical_scan` to process the sp1500_1d database using parallel processing. Return a structured list of tickers showing 'Active Breakout' or 'Coiling' signals."
)

fund_specialist = Agent(
    name="Fundamental Specialist",
    role="Filter technical candidates by verifying a sudden acceleration in earnings growth, acting as the breakout catalyst.",
    tools=[tool_verify_fundamental_inflection],
    model=Ollama(id=OLLAMA_MODEL_ID_FALLBACK),
    instructions="Take the list of tickers provided by the Technical Specialist and call `tool_verify_fundamental_inflection`. Only pass forward tickers that have a confirmed fundamental catalyst."
)

risk_manager = Agent(
    name="Risk Manager",
    role="Evaluate the final candidates for downside risk.",
    model=Ollama(id=OLLAMA_MODEL_ID),
    instructions="Review the final list. Provide a brief risk assessment for trading a 'Dormant Giant' breakout, emphasizing the importance of setting stop losses just below the breakout zone or the lower Bollinger Band."
)

# --- 4. Workflow Orchestration ---

team_lead = Team(
    name="Dormant Giant Screener Team Lead",
    members=[tech_specialist, fund_specialist, risk_manager],
    model=Ollama(id=OLLAMA_MODEL_ID),
    instructions="""
    Orchestrate the stock screening process:
    1. Ask the Technical Specialist to run the database scan.
    2. Pass the results to the Fundamental Specialist for EPS verification.
    3. Pass the surviving candidates to the Risk Manager for final trade parameters.
    4. Output a comprehensive final report summarizing the viable 'Dormant Giant' candidates.
    """,
    debug_mode=True,
    markdown=True
)

if __name__ == "__main__":
    print("Initiating Multi-Agent Dormant Giant Screener...")
    team_lead.print_response("Begin the daily Dormant Giant screening workflow across the S&P 1500 universe.", stream=True)
