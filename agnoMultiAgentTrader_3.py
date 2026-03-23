import psycopg2
import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor
from agno.agent import Agent
from agno.models.ollama import Ollama

# --- 1. Database Configuration & Helper Functions ---
DB_CONFIG = {
    'dbname': 'sp1500_1d',
    'user': 'your_username',
    'password': 'your_password',
    'host': 'localhost',
    'port': '5432'
}

def get_active_tickers():
    conn = psycopg2.connect(
        dbname=DB_CONFIG['dbname'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port']
    )
    try:
        query = "SELECT ticker FROM stock_metadata WHERE active = True;"
        tickers = pd.read_sql(query, conn)['ticker'].tolist()
    finally:
        conn.close()
    return tickers

# --- 2. Tool Definitions for the Agents ---

def analyze_single_ticker_technical(ticker: str) -> dict | None:
    """Worker function for ProcessPoolExecutor to analyze technicals."""
    try:
        conn = psycopg2.connect(
            dbname=DB_CONFIG['dbname'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port']
        )
        query = f"SELECT date, close, volume, high FROM {ticker.lower()} ORDER BY date DESC LIMIT 200;"
        df = pd.read_sql(query, conn).sort_values('date')
        conn.close()

        if len(df) < 120:
            return None

        # Squeeze Logic
        df['sma'] = df['close'].rolling(window=20).mean()
        df['std'] = df['close'].rolling(window=20).std()
        df['bandwidth'] = ((df['sma'] + (df['std'] * 2)) - (df['sma'] - (df['std'] * 2))) / df['sma']

        is_squeezing = df['bandwidth'].iloc[-1] <= (df['bandwidth'].tail(120).min() * 1.15)

        # OBV Logic
        close_diff = df['close'].diff()
        df['obv'] = (close_diff.sign() * df['volume']).fillna(0).cumsum()
        obv_slope = np.polyfit(np.arange(20), df['obv'].tail(20), 1)[0]
        price_slope = np.polyfit(np.arange(20), df['close'].tail(20), 1)[0]
        hidden_accumulation = (obv_slope > 0) and (abs(price_slope) < (df['close'].iloc[-1] * 0.005))

        # Breakout Logic
        past_resistance = df['high'].shift(3).rolling(window=120).max().iloc[-1]
        is_breakout = (df['close'].iloc[-1] > past_resistance) and (df['volume'].iloc[-1] > (df['volume'].tail(50).mean() * 1.5))

        if is_breakout:
            return {"ticker": ticker.upper(), "signal": "Active Breakout"}
        elif is_squeezing and hidden_accumulation:
            return {"ticker": ticker.upper(), "signal": "Coiling (Accumulation)"}

    except Exception:
        pass
    return None

def tool_run_technical_scan() -> list:
    """Tool for the Technical Specialist to run a parallelized database scan."""
    tickers = get_active_tickers()
    results = []

    # Using ProcessPoolExecutor for fast indicator calculation across PostgreSQL tables
    with ProcessPoolExecutor(max_workers=8) as executor:
        for result in executor.map(analyze_single_ticker_technical, tickers):
            if result:
                results.append(result)

    return results

def tool_verify_fundamental_inflection(tickers: list) -> list:
    """Tool for the Fundamental Specialist to check EPS acceleration."""
    conn = psycopg2.connect(
        dbname=DB_CONFIG['dbname'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port']
    )
    verified_tickers = []

    for item in tickers:
        ticker = item['ticker']
        try:
            query = f"""
                SELECT eps FROM stock_financials_quarterly
                WHERE ticker = '{ticker}' ORDER BY report_date DESC LIMIT 3;
            """
            fin_df = pd.read_sql(query, conn)

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

    conn.close()
    return verified_tickers

# --- 3. Agno Agent Initialization ---

tech_specialist = Agent(
    name="Technical Specialist",
    role="Identify stocks experiencing volatility contraction (Bollinger Squeeze), hidden institutional accumulation (OBV), or key resistance breakouts.",
    tools=[tool_run_technical_scan],
    model=Ollama(id="llama3.1"),
    instructions="Call the `tool_run_technical_scan` to process the sp1500_1d database using parallel processing. Return a structured list of tickers showing 'Active Breakout' or 'Coiling' signals."
)

fund_specialist = Agent(
    name="Fundamental Specialist",
    role="Filter technical candidates by verifying a sudden acceleration in earnings growth, acting as the breakout catalyst.",
    tools=[tool_verify_fundamental_inflection],
    model=Ollama(id="mistral"),
    instructions="Take the list of tickers provided by the Technical Specialist and call `tool_verify_fundamental_inflection`. Only pass forward tickers that have a confirmed fundamental catalyst."
)

risk_manager = Agent(
    name="Risk Manager",
    role="Evaluate the final candidates for downside risk.",
    model=Ollama(id="llama3.1"),
    instructions="Review the final list. Provide a brief risk assessment for trading a 'Dormant Giant' breakout, emphasizing the importance of setting stop losses just below the breakout zone or the lower Bollinger Band."
)

# --- 4. Workflow Orchestration ---

team_lead = Agent(
    name="Dormant Giant Screener Team Lead",
    team=[tech_specialist, fund_specialist, risk_manager],
    model=Ollama(id="llama3.1"),
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
