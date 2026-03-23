"""
plot_ohlcv.py — Interactive OHLCV candlestick + volume chart using Plotly.
Reads data from the local sp1500_1d PostgreSQL database.

Usage:
    python plot_ohlcv.py [TICKER] [--period DAYS]

Defaults:
    TICKER  = KLIC
    --period = 365  (last 365 calendar days; pass 0 for all history)
"""

import argparse
import sys
import pandas as pd
from sqlalchemy import create_engine, text
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ── TA Library Imports ─────────────────────────────────────────────────────────
# Note: Use aliases to avoid naming conflicts with wrapper functions
from ta.trend import (
    SMAIndicator, EMAIndicator, WMAIndicator, TRIXIndicator,
    MACD as MACDClass,  # Alias to avoid conflict with MACD() wrapper function
    AroonIndicator, CCIIndicator, ADXIndicator, VortexIndicator,
    PSARIndicator, STCIndicator, KSTIndicator, DPOIndicator,
    MassIndex, IchimokuIndicator,
)
from ta.momentum import (
    RSIIndicator, TSIIndicator, UltimateOscillator, StochasticOscillator,
    KAMAIndicator, ROCIndicator, AwesomeOscillatorIndicator,
    WilliamsRIndicator, StochRSIIndicator, PercentagePriceOscillator,
    PercentageVolumeOscillator,
)
from ta.volatility import (
    AverageTrueRange, BollingerBands, KeltnerChannel,
    DonchianChannel, UlcerIndex,
)
from ta.volume import (
    AccDistIndexIndicator, OnBalanceVolumeIndicator, ChaikinMoneyFlowIndicator,
    ForceIndexIndicator, EaseOfMovementIndicator, VolumePriceTrendIndicator,
    NegativeVolumeIndexIndicator, MFIIndicator, VolumeWeightedAveragePrice,
)
from ta.others import (
    DailyReturnIndicator, DailyLogReturnIndicator, CumulativeReturnIndicator,
)

# ── DB config (mirrors sp1500_database_technical.py) ─────────────────────────
DB_USER     = "postgres"
DB_PASSWORD = "sarina00"
DB_HOST     = "127.0.0.1"
DB_PORT     = "5431"
DB_NAME     = "sp1500_1d"


# ── Indicator Help Registry ─────────────────────────────────────────────────────
# Format: {name: (category, description, default_params, data_requirements)}

INDICATOR_HELP = {
    # Trend - overlay-friendly
    "SMA": ("Trend", "Simple Moving Average", {"period": 20}, ["close"]),
    "EMA": ("Trend", "Exponential Moving Average", {"period": 20}, ["close"]),
    "WMA": ("Trend", "Weighted Moving Average", {"period": 9}, ["close"]),
    "KAMA": ("Trend", "Kaufman's Adaptive Moving Average", {"window": 10}, ["close"]),
    "TRIX": ("Trend", "Triple EMA Momentum", {"window": 15}, ["close"]),
    "MACD": ("Trend", "MACD Line", {"window_fast": 12, "window_slow": 26, "window_sign": 9}, ["close"]),
    "MACD_signal": ("Trend", "MACD Signal Line", {"window_fast": 12, "window_slow": 26, "window_sign": 9}, ["close"]),
    "MACD_diff": ("Trend", "MACD Histogram", {"window_fast": 12, "window_slow": 26, "window_sign": 9}, ["close"]),
    "ADX": ("Trend", "Average Directional Index", {"window": 14}, ["high", "low", "close"]),
    "ADX_pos": ("Trend", "ADX Positive Directional", {"window": 14}, ["high", "low", "close"]),
    "ADX_neg": ("Trend", "ADX Negative Directional", {"window": 14}, ["high", "low", "close"]),
    "Aroon_up": ("Trend", "Aroon Up", {"window": 25}, ["high", "low"]),
    "Aroon_down": ("Trend", "Aroon Down", {"window": 25}, ["high", "low"]),
    "CCI": ("Trend", "Commodity Channel Index", {"window": 20}, ["high", "low", "close"]),
    "DPO": ("Trend", "Detrended Price Oscillator", {"window": 20}, ["close"]),
    "KST": ("Trend", "Know Sure Thing", {}, ["close"]),
    "KST_sig": ("Trend", "KST Signal", {}, ["close"]),
    "MassIndex": ("Trend", "Mass Index", {"window_fast": 9, "window_slow": 25}, ["high", "low"]),
    "Vortex_pos": ("Trend", "Vortex Positive", {"window": 14}, ["high", "low", "close"]),
    "Vortex_neg": ("Trend", "Vortex Negative", {"window": 14}, ["high", "low", "close"]),
    "STC": ("Trend", "Schaff Trend Cycle", {}, ["close"]),
    "PSAR": ("Trend", "Parabolic SAR", {"step": 0.02, "max_step": 0.2}, ["high", "low", "close"]),
    "Ichimoku_conv": ("Trend", "Ichimoku Conversion Line", {"window1": 9, "window2": 26}, ["high", "low"]),
    "Ichimoku_base": ("Trend", "Ichimoku Base Line", {"window1": 9, "window2": 26}, ["high", "low"]),
    "Ichimoku_a": ("Trend", "Ichimoku Span A", {"window1": 9, "window2": 26}, ["high", "low"]),
    "Ichimoku_b": ("Trend", "Ichimoku Span B", {"window2": 26, "window3": 52}, ["high", "low"]),

    # Momentum - separate recommended
    "RSI": ("Momentum", "Relative Strength Index", {"window": 14}, ["close"]),
    "TSI": ("Momentum", "True Strength Index", {"window_slow": 25, "window_fast": 13}, ["close"]),
    "Stochastic": ("Momentum", "Stochastic Oscillator", {"window": 14, "smooth_window": 3}, ["high", "low", "close"]),
    "Stochastic_signal": ("Momentum", "Stochastic Signal", {"window": 14, "smooth_window": 3}, ["high", "low", "close"]),
    "Williams_R": ("Momentum", "Williams %R", {"lbp": 14}, ["high", "low", "close"]),
    "AwesomeOsc": ("Momentum", "Awesome Oscillator", {"window1": 5, "window2": 34}, ["high", "low"]),
    "ROC": ("Momentum", "Rate of Change", {"window": 12}, ["close"]),
    "StochRSI": ("Momentum", "Stochastic RSI", {"window": 14}, ["close"]),
    "StochRSI_k": ("Momentum", "Stochastic RSI %K", {"window": 14}, ["close"]),
    "StochRSI_d": ("Momentum", "Stochastic RSI %D", {"window": 14}, ["close"]),
    "UltimateOsc": ("Momentum", "Ultimate Oscillator", {"window1": 7, "window2": 14, "window3": 28}, ["high", "low", "close"]),
    "PPO": ("Momentum", "Percentage Price Oscillator", {"window_slow": 26, "window_fast": 12, "window_sign": 9}, ["close"]),
    "PPO_signal": ("Momentum", "PPO Signal Line", {"window_slow": 26, "window_fast": 12, "window_sign": 9}, ["close"]),
    "PPO_hist": ("Momentum", "PPO Histogram", {"window_slow": 26, "window_fast": 12, "window_sign": 9}, ["close"]),

    # Volume
    "OBV": ("Volume", "On-Balance Volume", {}, ["close", "volume"]),
    "MFI": ("Volume", "Money Flow Index", {"window": 14}, ["high", "low", "close", "volume"]),
    "CMF": ("Volume", "Chaikin Money Flow", {"window": 20}, ["high", "low", "close", "volume"]),
    "VWAP": ("Volume", "Volume Weighted Average Price", {"window": 14}, ["high", "low", "close", "volume"]),
    "ADI": ("Volume", "Accumulation/Distribution", {}, ["high", "low", "close", "volume"]),
    "ForceIndex": ("Volume", "Force Index", {"window": 13}, ["close", "volume"]),
    "VPT": ("Volume", "Volume Price Trend", {}, ["close", "volume"]),
    "NVI": ("Volume", "Negative Volume Index", {}, ["close", "volume"]),
    "EaseOfMovement": ("Volume", "Ease of Movement", {"window": 14}, ["high", "low", "volume"]),
    "PVO": ("Volume", "Percentage Volume Oscillator", {"window_slow": 26, "window_fast": 12, "window_sign": 9}, ["volume"]),
    "PVO_signal": ("Volume", "PVO Signal Line", {"window_slow": 26, "window_fast": 12, "window_sign": 9}, ["volume"]),
    "PVO_hist": ("Volume", "PVO Histogram", {"window_slow": 26, "window_fast": 12, "window_sign": 9}, ["volume"]),

    # Volatility
    "ATR": ("Volatility", "Average True Range", {"window": 14}, ["high", "low", "close"]),
    "Bollinger_mavg": ("Volatility", "Bollinger Middle Band", {"window": 20}, ["close"]),
    "Bollinger_hband": ("Volatility", "Bollinger Upper Band", {"window": 20, "window_dev": 2}, ["close"]),
    "Bollinger_lband": ("Volatility", "Bollinger Lower Band", {"window": 20, "window_dev": 2}, ["close"]),
    "Bollinger_wband": ("Volatility", "Bollinger Band Width", {"window": 20, "window_dev": 2}, ["close"]),
    "Bollinger_pband": ("Volatility", "Bollinger Percentage Band", {"window": 20, "window_dev": 2}, ["close"]),
    "Keltner_mband": ("Volatility", "Keltner Middle Band", {"window": 20}, ["high", "low", "close"]),
    "Keltner_hband": ("Volatility", "Keltner Upper Band", {"window": 20}, ["high", "low", "close"]),
    "Keltner_lband": ("Volatility", "Keltner Lower Band", {"window": 20}, ["high", "low", "close"]),
    "Donchian_hband": ("Volatility", "Donchian Upper Band", {"window": 20}, ["high", "low"]),
    "Donchian_lband": ("Volatility", "Donchian Lower Band", {"window": 20}, ["high", "low"]),
    "Donchian_mband": ("Volatility", "Donchian Middle Band", {"window": 20}, ["high", "low"]),
    "UlcerIndex": ("Volatility", "Ulcer Index", {"window": 14}, ["close"]),

    # Others
    "DailyReturn": ("Others", "Daily Return", {}, ["close"]),
    "DailyLogReturn": ("Others", "Daily Log Return", {}, ["close"]),
    "CumulativeReturn": ("Others", "Cumulative Return", {}, ["close"]),
}

# Default parameters for indicators
INDICATOR_DEFAULTS = {name: info[2] for name, info in INDICATOR_HELP.items()}

# Indicators that work well as overlays on price chart
OVERLAY_INDICATORS = {
    "SMA", "EMA", "WMA", "KAMA", "TRIX", "MACD", "MACD_signal", "MACD_diff",
    "PSAR", "Ichimoku_conv", "Ichimoku_base", "Ichimoku_a", "Ichimoku_b",
    "VWAP", "Bollinger_mavg", "Bollinger_hband", "Bollinger_lband",
    "Keltner_mband", "Keltner_hband", "Keltner_lband",
    "Donchian_hband", "Donchian_lband", "Donchian_mband",
}


def load_ohlcv(ticker: str, days: int = 365) -> pd.DataFrame:
    table = ticker.lower().replace(".", "-")
    conn_str = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(conn_str)

    with engine.connect() as conn:
        if days > 0:
            query = text(
                f'SELECT * FROM "{table}" '
                f'WHERE "Date" >= NOW() - INTERVAL \'{days} days\' '
                f'ORDER BY "Date" ASC'
            )
        else:
            query = text(f'SELECT * FROM "{table}" ORDER BY "Date" ASC')
        df = pd.read_sql(query, conn, index_col="Date", parse_dates=["Date"])

    return df


# ── Indicator Wrapper Functions (for IDE hover support in Zed) ─────────────────
# These functions provide docstrings for IDE hover tooltips

def SMA(close: pd.Series, period: int = 20) -> pd.Series:
    """Simple Moving Average (SMA)

    A simple moving average that smooths price data over a specified period.

    Args:
        close: Close prices Series
        period: Number of periods (default: 20)

    Returns:
        SMA values as Series
    """
    return SMAIndicator(close=close, window=period, fillna=False).sma_indicator()


def EMA(close: pd.Series, period: int = 20) -> pd.Series:
    """Exponential Moving Average (EMA)

    An exponentially weighted moving average that gives more weight to recent prices.

    Args:
        close: Close prices Series
        period: Number of periods (default: 20)

    Returns:
        EMA values as Series
    """
    return EMAIndicator(close=close, window=period, fillna=False).ema_indicator()


def RSI(close: pd.Series, period: int = 14) -> pd.Series:
    """Relative Strength Index (RSI)

    Momentum oscillator measuring speed and change of price movements.
    Values 0-100. Above 70 = overbought, below 30 = oversold.

    Args:
        close: Close prices Series
        period: Number of periods (default: 14)

    Returns:
        RSI values (0-100)
    """
    return RSIIndicator(close=close, window=period, fillna=False).rsi()


def MACD(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.Series:
    """Moving Average Convergence Divergence (MACD)

    Trend-following momentum indicator showing relationship between two EMAs.

    Args:
        close: Close prices Series
        fast: Fast EMA period (default: 12)
        slow: Slow EMA period (default: 26)
        signal: Signal line period (default: 9)

    Returns:
        MACD line values
    """
    # Use MACDClass (the alias) to avoid recursion
    return MACDClass(close=close, window_fast=fast, window_slow=slow, window_sign=signal, fillna=False).macd()


def MACD_signal(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.Series:
    """MACD Signal Line

    EMA of the MACD line, used for signal generation.

    Args:
        close: Close prices Series
        fast: Fast EMA period (default: 12)
        slow: Slow EMA period (default: 26)
        signal: Signal line period (default: 9)

    Returns:
        MACD signal line values
    """
    return MACDClass(close=close, window_fast=fast, window_slow=slow, window_sign=signal, fillna=False).macd_signal()


def MACD_diff(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.Series:
    """MACD Histogram

    The difference between MACD and its signal line.

    Args:
        close: Close prices Series
        fast: Fast EMA period (default: 12)
        slow: Slow EMA period (default: 26)
        signal: Signal line period (default: 9)

    Returns:
        MACD histogram values
    """
    return MACDClass(close=close, window_fast=fast, window_slow=slow, window_sign=signal, fillna=False).macd_diff()


def Bollinger_hband(close: pd.Series, period: int = 20, dev: float = 2.0) -> pd.Series:
    """Bollinger Bands - Upper Band

    Upper band at K times an N-period standard deviation above the MA.

    Args:
        close: Close prices Series
        period: Number of periods (default: 20)
        dev: Standard deviation multiplier (default: 2.0)

    Returns:
        Upper Bollinger Band values
    """
    return BollingerBands(close=close, window=period, window_dev=dev, fillna=False).bollinger_hband()


def Bollinger_lband(close: pd.Series, period: int = 20, dev: float = 2.0) -> pd.Series:
    """Bollinger Bands - Lower Band

    Lower band at K times an N-period standard deviation below the MA.

    Args:
        close: Close prices Series
        period: Number of periods (default: 20)
        dev: Standard deviation multiplier (default: 2.0)

    Returns:
        Lower Bollinger Band values
    """
    return BollingerBands(close=close, window=period, window_dev=dev, fillna=False).bollinger_lband()


def ATR(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    """Average True Range (ATR)

    Measures market volatility. High ATR = high volatility.

    Args:
        high: High prices Series
        low: Low prices Series
        close: Close prices Series
        period: Number of periods (default: 14)

    Returns:
        ATR values
    """
    return AverageTrueRange(high=high, low=low, close=close, window=period, fillna=False).average_true_range()


def VWAP(high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series, period: int = 14) -> pd.Series:
    """Volume Weighted Average Price (VWAP)

    Average price weighted by volume, used to assess trading efficiency.

    Args:
        high: High prices Series
        low: Low prices Series
        close: Close prices Series
        volume: Volume Series
        period: Number of periods (default: 14)

    Returns:
        VWAP values
    """
    return VolumeWeightedAveragePrice(high=high, low=low, close=close, volume=volume,
                                       window=period, fillna=False).volume_weighted_average_price()


def validate_data_columns(df: pd.DataFrame, required_cols: list) -> None:
    """Validate that required OHLCV columns exist.

    Args:
        df: DataFrame with OHLCV data
        required_cols: List of required column names

    Raises:
        ValueError: If required columns are missing
    """
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(
            f"Missing required columns for indicator: {missing}. "
            f"Available: {list(df.columns)}"
        )


def calculate_indicator(df: pd.DataFrame, name: str, params: dict) -> pd.Series:
    """Calculate a technical indicator from OHLCV data.

    Args:
        df: DataFrame with Open, High, Low, Close, Volume columns
        name: Indicator name (e.g., 'RSI', 'SMA')
        params: Dictionary of parameters for the indicator

    Returns:
        Series with calculated indicator values
    """
    # Get required data columns from INDICATOR_HELP
    if name not in INDICATOR_HELP:
        raise ValueError(f"Unknown indicator: {name}")

    _, _, _, data_reqs = INDICATOR_HELP[name]
    validate_data_columns(df, data_reqs)

    # Get defaults and merge with user params
    defaults = INDICATOR_DEFAULTS.get(name, {})
    params = {**defaults, **params}

    close = df["close"]
    high = df.get("high")
    low = df.get("low")
    volume = df.get("volume")

    # ── Trend Indicators ─────────────────────────────────────────────────────────
    if name == "SMA":
        return SMAIndicator(close=close, window=params.get("period", 20), fillna=False).sma_indicator()
    elif name == "EMA":
        return EMAIndicator(close=close, window=params.get("period", 20), fillna=False).ema_indicator()
    elif name == "WMA":
        return WMAIndicator(close=close, window=params.get("period", 9), fillna=False).wma()
    elif name == "KAMA":
        return KAMAIndicator(close=close, window=params.get("window", 10), fillna=False).kama()
    elif name == "TRIX":
        return TRIXIndicator(close=close, window=params.get("window", 15), fillna=False).trix()
    elif name == "MACD":
        return MACDClass(close=close, window_fast=params.get("window_fast", 12),
                         window_slow=params.get("window_slow", 26),
                         window_sign=params.get("window_sign", 9), fillna=False).macd()
    elif name == "MACD_signal":
        return MACDClass(close=close, window_fast=params.get("window_fast", 12),
                         window_slow=params.get("window_slow", 26),
                         window_sign=params.get("window_sign", 9), fillna=False).macd_signal()
    elif name == "MACD_diff":
        return MACDClass(close=close, window_fast=params.get("window_fast", 12),
                         window_slow=params.get("window_slow", 26),
                         window_sign=params.get("window_sign", 9), fillna=False).macd_diff()
    elif name == "ADX":
        return ADXIndicator(high=high, low=low, close=close, window=params.get("window", 14), fillna=False).adx()
    elif name == "ADX_pos":
        return ADXIndicator(high=high, low=low, close=close, window=params.get("window", 14), fillna=False).adx_pos()
    elif name == "ADX_neg":
        return ADXIndicator(high=high, low=low, close=close, window=params.get("window", 14), fillna=False).adx_neg()
    elif name == "Aroon_up":
        return AroonIndicator(high=high, low=low, window=params.get("window", 25), fillna=False).aroon_up()
    elif name == "Aroon_down":
        return AroonIndicator(high=high, low=low, window=params.get("window", 25), fillna=False).aroon_down()
    elif name == "CCI":
        return CCIIndicator(high=high, low=low, close=close, window=params.get("window", 20), fillna=False).cci()
    elif name == "DPO":
        return DPOIndicator(close=close, window=params.get("window", 20), fillna=False).dpo()
    elif name == "KST":
        return KSTIndicator(close=close, fillna=False).kst()
    elif name == "KST_sig":
        return KSTIndicator(close=close, fillna=False).kst_sig()
    elif name == "MassIndex":
        return MassIndex(high=high, low=low, window_fast=params.get("window_fast", 9),
                        window_slow=params.get("window_slow", 25), fillna=False).mass_index()
    elif name == "Vortex_pos":
        return VortexIndicator(high=high, low=low, close=close, window=params.get("window", 14), fillna=False).vortex_pos()
    elif name == "Vortex_neg":
        return VortexIndicator(high=high, low=low, close=close, window=params.get("window", 14), fillna=False).vortex_neg()
    elif name == "STC":
        return STCIndicator(close=close, fillna=False).stc()
    elif name == "PSAR":
        return PSARIndicator(high=high, low=low, step=params.get("step", 0.02),
                             max_step=params.get("max_step", 0.2), fillna=False).psar()
    elif name == "Ichimoku_conv":
        return IchimokuIndicator(high=high, low=low, window1=params.get("window1", 9),
                                  window2=params.get("window2", 26), fillna=False).ichimoku_conversion_line()
    elif name == "Ichimoku_base":
        return IchimokuIndicator(high=high, low=low, window1=params.get("window1", 9),
                                  window2=params.get("window2", 26), fillna=False).ichimoku_base_line()
    elif name == "Ichimoku_a":
        return IchimokuIndicator(high=high, low=low, window1=params.get("window1", 9),
                                  window2=params.get("window2", 26), fillna=False).ichimoku_span_a()
    elif name == "Ichimoku_b":
        return IchimokuIndicator(high=high, low=low, window2=params.get("window2", 26),
                                  window3=params.get("window3", 52), fillna=False).ichimoku_span_b()

    # ── Momentum Indicators ──────────────────────────────────────────────────────
    elif name == "RSI":
        return RSIIndicator(close=close, window=params.get("window", 14), fillna=False).rsi()
    elif name == "TSI":
        return TSIIndicator(close=close, window_slow=params.get("window_slow", 25),
                            window_fast=params.get("window_fast", 13), fillna=False).tsi()
    elif name == "Stochastic":
        return StochasticOscillator(high=high, low=low, close=close,
                                     window=params.get("window", 14),
                                     smooth_window=params.get("smooth_window", 3), fillna=False).stoch()
    elif name == "Stochastic_signal":
        return StochasticOscillator(high=high, low=low, close=close,
                                     window=params.get("window", 14),
                                     smooth_window=params.get("smooth_window", 3), fillna=False).stoch_signal()
    elif name == "Williams_R":
        return WilliamsRIndicator(high=high, low=low, close=close, lbp=params.get("lbp", 14), fillna=False).williams_r()
    elif name == "AwesomeOsc":
        return AwesomeOscillatorIndicator(high=high, low=low, window1=params.get("window1", 5),
                                          window2=params.get("window2", 34), fillna=False).awesome_oscillator()
    elif name == "ROC":
        return ROCIndicator(close=close, window=params.get("window", 12), fillna=False).roc()
    elif name == "StochRSI":
        return StochRSIIndicator(close=close, window=params.get("window", 14), fillna=False).stoch_rsi()
    elif name == "StochRSI_k":
        return StochRSIIndicator(close=close, window=params.get("window", 14), fillna=False).stoch_rsi_k()
    elif name == "StochRSI_d":
        return StochRSIIndicator(close=close, window=params.get("window", 14), fillna=False).stoch_rsi_d()
    elif name == "UltimateOsc":
        return UltimateOscillator(high=high, low=low, close=close,
                                   window1=params.get("window1", 7),
                                   window2=params.get("window2", 14),
                                   window3=params.get("window3", 28), fillna=False).ultimate_oscillator()
    elif name == "PPO":
        return PercentagePriceOscillator(close=close, window_slow=params.get("window_slow", 26),
                                         window_fast=params.get("window_fast", 12),
                                         window_sign=params.get("window_sign", 9), fillna=False).ppo()
    elif name == "PPO_signal":
        return PercentagePriceOscillator(close=close, window_slow=params.get("window_slow", 26),
                                         window_fast=params.get("window_fast", 12),
                                         window_sign=params.get("window_sign", 9), fillna=False).ppo_signal()
    elif name == "PPO_hist":
        return PercentagePriceOscillator(close=close, window_slow=params.get("window_slow", 26),
                                         window_fast=params.get("window_fast", 12),
                                         window_sign=params.get("window_sign", 9), fillna=False).ppo_hist()

    # ── Volume Indicators ────────────────────────────────────────────────────────
    elif name == "OBV":
        return OnBalanceVolumeIndicator(close=close, volume=volume, fillna=False).on_balance_volume()
    elif name == "MFI":
        return MFIIndicator(high=high, low=low, close=close, volume=volume,
                           window=params.get("window", 14), fillna=False).money_flow_index()
    elif name == "CMF":
        return ChaikinMoneyFlowIndicator(high=high, low=low, close=close, volume=volume,
                                         window=params.get("window", 20), fillna=False).chaikin_money_flow()
    elif name == "VWAP":
        return VolumeWeightedAveragePrice(high=high, low=low, close=close, volume=volume,
                                          window=params.get("window", 14), fillna=False).volume_weighted_average_price()
    elif name == "ADI":
        return AccDistIndexIndicator(high=high, low=low, close=close, volume=volume, fillna=False).accumulation_distribution()
    elif name == "ForceIndex":
        return ForceIndexIndicator(close=close, volume=volume, window=params.get("window", 13), fillna=False).force_index()
    elif name == "VPT":
        return VolumePriceTrendIndicator(close=close, volume=volume, fillna=False).volume_price_trend()
    elif name == "NVI":
        return NegativeVolumeIndexIndicator(close=close, volume=volume, fillna=False).negative_volume_index()
    elif name == "EaseOfMovement":
        return EaseOfMovementIndicator(high=high, low=low, volume=volume, window=params.get("window", 14), fillna=False).ease_of_movement()
    elif name == "PVO":
        return PercentageVolumeOscillator(volume=volume, window_slow=params.get("window_slow", 26),
                                           window_fast=params.get("window_fast", 12),
                                           window_sign=params.get("window_sign", 9), fillna=False).pvo()
    elif name == "PVO_signal":
        return PercentageVolumeOscillator(volume=volume, window_slow=params.get("window_slow", 26),
                                           window_fast=params.get("window_fast", 12),
                                           window_sign=params.get("window_sign", 9), fillna=False).pvo_signal()
    elif name == "PVO_hist":
        return PercentageVolumeOscillator(volume=volume, window_slow=params.get("window_slow", 26),
                                           window_fast=params.get("window_fast", 12),
                                           window_sign=params.get("window_sign", 9), fillna=False).pvo_hist()

    # ── Volatility Indicators ────────────────────────────────────────────────────
    elif name == "ATR":
        return AverageTrueRange(high=high, low=low, close=close, window=params.get("window", 14), fillna=False).average_true_range()
    elif name == "Bollinger_mavg":
        return BollingerBands(close=close, window=params.get("window", 20),
                              window_dev=params.get("window_dev", 2), fillna=False).bollinger_mavg()
    elif name == "Bollinger_hband":
        return BollingerBands(close=close, window=params.get("window", 20),
                              window_dev=params.get("window_dev", 2), fillna=False).bollinger_hband()
    elif name == "Bollinger_lband":
        return BollingerBands(close=close, window=params.get("window", 20),
                              window_dev=params.get("window_dev", 2), fillna=False).bollinger_lband()
    elif name == "Bollinger_wband":
        return BollingerBands(close=close, window=params.get("window", 20),
                              window_dev=params.get("window_dev", 2), fillna=False).bollinger_wband()
    elif name == "Bollinger_pband":
        return BollingerBands(close=close, window=params.get("window", 20),
                              window_dev=params.get("window_dev", 2), fillna=False).bollinger_pband()
    elif name == "Keltner_mband":
        return KeltnerChannel(high=high, low=low, close=close, window=params.get("window", 20), fillna=False).keltner_mband()
    elif name == "Keltner_hband":
        return KeltnerChannel(high=high, low=low, close=close, window=params.get("window", 20), fillna=False).keltner_hband()
    elif name == "Keltner_lband":
        return KeltnerChannel(high=high, low=low, close=close, window=params.get("window", 20), fillna=False).keltner_lband()
    elif name == "Donchian_hband":
        return DonchianChannel(high=high, low=low, window=params.get("window", 20), fillna=False).donchian_channel_hband()
    elif name == "Donchian_lband":
        return DonchianChannel(high=high, low=low, window=params.get("window", 20), fillna=False).donchian_channel_lband()
    elif name == "Donchian_mband":
        return DonchianChannel(high=high, low=low, window=params.get("window", 20), fillna=False).donchian_channel_mband()
    elif name == "UlcerIndex":
        return UlcerIndex(close=close, window=params.get("window", 14), fillna=False).ulcer_index()

    # ── Others ────────────────────────────────────────────────────────────────────
    elif name == "DailyReturn":
        return DailyReturnIndicator(close=close, fillna=False).daily_return()
    elif name == "DailyLogReturn":
        return DailyLogReturnIndicator(close=close, fillna=False).daily_log_return()
    elif name == "CumulativeReturn":
        return CumulativeReturnIndicator(close=close, fillna=False).cumulative_return()

    else:
        raise ValueError(f"Indicator {name} not yet implemented in calculate_indicator")


def parse_indicator_arg(arg: str) -> dict:
    """Parse --indicator argument.

    Args:
        arg: String in format "NAME:MODE[:PARAM=VALUE,...]"

    Returns:
        Dict with keys: name, mode, params
    """
    parts = arg.split(":")
    if len(parts) < 2:
        raise ValueError(f"Invalid indicator format: {arg}. Use NAME:MODE[:params]")

    name = parts[0].upper()
    mode = parts[1].lower()

    if mode not in ("overlay", "separate"):
        raise ValueError(f"Invalid mode '{mode}'. Use 'overlay' or 'separate'")

    # Parse optional params
    params = {}
    if len(parts) > 2:
        param_str = parts[2]
        for param in param_str.split(","):
            if "=" in param:
                key, value = param.split("=", 1)
                try:
                    if "." in value:
                        params[key] = float(value)
                    else:
                        params[key] = int(value)
                except ValueError:
                    params[key] = value

    return {"name": name, "mode": mode, "params": params}


def show_indicator_help(name: str = None):
    """Show help for indicator(s)."""
    if name:
        name = name.upper()
        if name not in INDICATOR_HELP:
            print(f"Unknown indicator: {name}")
            print(f"Run with --help-indicators to see all available indicators.")
            return

        category, description, defaults, data_reqs = INDICATOR_HELP[name]
        print(f"\n{name} - {description}")
        print(f"  Category: {category}")
        print(f"  Data required: {', '.join(data_reqs)}")
        if defaults:
            print(f"  Default parameters:")
            for k, v in defaults.items():
                print(f"    {k}={v}")
    else:
        # Show all indicators grouped by category
        categories = {}
        for ind_name, info in INDICATOR_HELP.items():
            cat = info[0]
            if cat not in categories:
                categories[cat] = []
            categories[cat].append((ind_name, info[1]))

        for cat, indicators in categories.items():
            print(f"\n{cat}:")
            for ind_name, description in indicators:
                overlay = " [overlay]" if ind_name in OVERLAY_INDICATORS else ""
                print(f"  {ind_name}: {description}{overlay}")
        print("\nUse --indicator-help NAME for details on a specific indicator.")


def plot(df: pd.DataFrame, ticker: str, indicators: list = None) -> None:
    """Plot OHLCV data with optional indicators.

    Args:
        df: DataFrame with OHLCV data
        ticker: Ticker symbol
        indicators: List of parsed indicator dicts from parse_indicator_arg()
    """
    # Normalise column names (strip whitespace and lowercase for indicator compatibility)
    df.columns = [c.strip().lower() for c in df.columns]

    # Separate overlay and separate indicators
    overlay_inds = []
    separate_inds = []
    if indicators:
        for ind in indicators:
            if ind["mode"] == "overlay":
                overlay_inds.append(ind)
            else:
                separate_inds.append(ind)

    # Calculate row count
    # Row 1: price (candlestick) + overlay indicators
    # Row 2: volume
    # Row 3+: separate indicators
    num_rows = 2 + len(separate_inds)

    # Calculate row heights
    if num_rows == 2:
        # Original behavior
        row_heights = [0.75, 0.25]
    else:
        # Dynamic heights
        price_height = 0.45
        volume_height = 0.15
        remaining = 0.40
        sep_height = remaining / len(separate_inds) if separate_inds else 0
        row_heights = [price_height, volume_height] + [sep_height] * len(separate_inds)

    # Create subplots
    fig = make_subplots(
        rows=num_rows, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=row_heights,
    )

    # Row 1: Candlestick + overlay indicators
    fig.add_trace(
        go.Candlestick(
            x=df.index,
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"],
            name=ticker,
            increasing_line_color="#26a69a",
            decreasing_line_color="#ef5350",
        ),
        row=1, col=1,
    )

    # Add overlay indicators to price row
    for ind in overlay_inds:
        try:
            series = calculate_indicator(df, ind["name"], ind["params"])
            fig.add_trace(
                go.Scatter(
                    x=df.index,
                    y=series,
                    name=ind["name"],
                    line=dict(width=1.5),
                ),
                row=1, col=1,
            )
        except Exception as e:
            print(f"Warning: Could not calculate {ind['name']}: {e}", file=sys.stderr)

    # Row 2: Volume
    colors = ["#26a69a" if c >= o else "#ef5350"
              for c, o in zip(df["close"], df["open"])]

    # For large datasets: hybrid approach (recent bars + historical MA)
    if len(df) > 1500:
        # Calculate volume MA for historical data
        ma_period = min(50, max(20, len(df) // 100))
        df["volume_ma"] = df["volume"].rolling(window=ma_period, min_periods=1).mean()

        # Split point: show last 500 days as bars (or fewer if total < 500)
        bar_count = min(500, len(df) // 4)  # At least 1/4 of data as bars, max 500
        split_idx = len(df) - bar_count

        # Plot bars FIRST (so they appear on top)
        fig.add_trace(
            go.Bar(
                x=df.index[split_idx:],
                y=df["volume"].iloc[split_idx:].fillna(0),
                name="Volume",
                marker_color=colors[split_idx:],
                opacity=0.8,
            ),
            row=2, col=1,
        )

        # Plot historical MA (line) for older data - no fill to avoid obscuring
        fig.add_trace(
            go.Scatter(
                x=df.index[:split_idx],
                y=df["volume_ma"].iloc[:split_idx].fillna(0),
                name=f"Volume MA({ma_period})",
                mode="lines",
                line=dict(color="#78909c", width=1.5),  # Gray for historical
            ),
            row=2, col=1,
        )
    else:
        # Small dataset: show raw volume bars
        fig.add_trace(
            go.Bar(
                x=df.index,
                y=df["volume"],
                name="Volume",
                marker_color=colors,
                opacity=0.7,
            ),
            row=2, col=1,
        )

    # Rows 3+: Separate indicators
    for i, ind in enumerate(separate_inds, start=3):
        try:
            series = calculate_indicator(df, ind["name"], ind["params"])
            fig.add_trace(
                go.Scatter(
                    x=df.index,
                    y=series,
                    name=ind["name"],
                    line=dict(width=1.5),
                ),
                row=i, col=1,
            )
            # Add reference lines for certain indicators
            if ind["name"] in ("RSI", "Stochastic", "StochRSI", "Williams_R"):
                fig.add_hline(y=70, line_dash="dash", line_color="gray", row=i, col=1)
                fig.add_hline(y=30, line_dash="dash", line_color="gray", row=i, col=1)
            elif ind["name"] in ("MACD", "MACD_signal", "MACD_diff", "PPO", "PPO_hist"):
                fig.add_hline(y=0, line_dash="dash", line_color="gray", row=i, col=1)
        except Exception as e:
            print(f"Warning: Could not calculate {ind['name']}: {e}", file=sys.stderr)

    # Update layout
    yaxis_titles = ["Price (USD)", "Volume"]
    for ind in separate_inds:
        yaxis_titles.append(ind["name"])

    fig.update_layout(
        title=dict(text=f"{ticker} — OHLCV" + (f" + {len(overlay_inds) + len(separate_inds)} indicators" if indicators else ""),
                   font=dict(size=20)),
        xaxis_rangeslider_visible=False,
        template="plotly_dark",
        height=300 + (num_rows * 100),  # Dynamic height
        legend=dict(orientation="h", y=1.02, x=0),
    )

    # Set y-axis titles
    for i, title in enumerate(yaxis_titles, start=1):
        fig.update_layout(**{f"yaxis{i}_title": title})

    # Remove weekend/holiday gaps
    fig.update_xaxes(
        rangebreaks=[dict(bounds=["sat", "mon"])]
    )

    fig.show()


def main():
    parser = argparse.ArgumentParser(
        description="Plot OHLCV data for a ticker with optional technical indicators.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s AAPL                                    # Basic plot
  %(prog)s AAPL --period 180                      # Last 180 days
  %(prog)s AAPL --indicator RSI:separate          # RSI in separate row
  %(prog)s AAPL --indicator SMA:overlay:period=50 # 50-day SMA on price
  %(prog)s AAPL --indicator MACD:separate --indicator RSI:separate
  %(prog)s --help-indicators                       # Show all indicators
  %(prog)s --indicator-help RSI                   # Show RSI help

Indicator format: NAME:MODE[:PARAM=VALUE,...]
  NAME:    Indicator name (e.g., RSI, SMA, MACD)
  MODE:    overlay or separate
  PARAM:   Optional parameters (e.g., period=14, fast=12)
"""
    )
    parser.add_argument("ticker", nargs="?", default="KLIC",
                        help="Ticker symbol (default: KLIC)")
    parser.add_argument("--period", type=int, default=365,
                        help="Number of calendar days to plot (0 = all history, default: 365)")
    parser.add_argument("--indicator", "-i", action="append", dest="indicators",
                        help="Add indicator (can be specified multiple times)")
    parser.add_argument("--help-indicators", action="store_true",
                        help="Show all available indicators")
    parser.add_argument("--indicator-help", dest="indicator_help",
                        help="Show help for a specific indicator")

    args = parser.parse_args()

    # Handle help flags
    if args.help_indicators:
        show_indicator_help()
        return

    if args.indicator_help:
        show_indicator_help(args.indicator_help)
        return

    # Parse indicators
    indicators = None
    if args.indicators:
        indicators = []
        for ind_arg in args.indicators:
            try:
                indicators.append(parse_indicator_arg(ind_arg))
            except ValueError as e:
                print(f"Error: {e}", file=sys.stderr)
                sys.exit(1)

    ticker = args.ticker.upper()
    period_str = "all" if args.period == 0 else str(args.period)
    print(f"Loading {ticker} — last {period_str} days...")

    # Load data
    try:
        df = load_ohlcv(ticker, args.period)
    except Exception as e:
        print(f"Error loading data for {ticker}: {e}", file=sys.stderr)
        sys.exit(1)

    if df.empty:
        print(f"No data found for {ticker}.", file=sys.stderr)
        sys.exit(1)

    print(f"Loaded {len(df)} rows  ({df.index[0].date()} → {df.index[-1].date()})")

    # Show indicator summary
    if indicators:
        print(f"Indicators: {', '.join([i['name'] for i in indicators])}")

    # Plot
    plot(df, ticker, indicators)


if __name__ == "__main__":
    main()
