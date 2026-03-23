"""
chart_tradingview.py — TradingView-style interactive OHLCV charts using Streamlit + Lightweight Charts.
With technical indicator support from the ta library.
"""

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from streamlit_lightweight_charts_pro import (
    Chart, CandlestickSeries, HistogramSeries, LineSeries,
    ChartOptions, LayoutOptions, PaneHeightOptions
)
from streamlit_lightweight_charts_pro.data import CandlestickData, HistogramData, LineData
import datetime
import sys
import os
import re

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import from plot_ohlcv.py - reuse indicator definitions and calculation logic
from plot_ohlcv import INDICATOR_HELP, OVERLAY_INDICATORS, calculate_indicator

# --- Database Configuration ---
# Use environment variables for credentials with defaults for local development
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "sarina00")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5431")
DB_NAME = os.getenv("DB_NAME", "sp1500_1d")


# --- Ticker Validation ---
# Define valid ticker pattern: uppercase letters, numbers, and dots (for BRK.B type tickers)
VALID_TICKER_PATTERN = re.compile(r'^[A-Z0-9\.]+$')


def is_valid_ticker(ticker: str) -> bool:
    """Validate ticker format.

    Args:
        ticker: Ticker string to validate

    Returns:
        True if ticker matches valid pattern, False otherwise
    """
    return bool(VALID_TICKER_PATTERN.match(ticker)) and len(ticker) <= 10


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize DataFrame columns to lowercase stripped format.

    This helper function ensures consistent column naming across all data
    preparation functions, matching the expected format for the ta library.

    Args:
        df: DataFrame with OHLCV columns

    Returns:
        DataFrame with normalized column names (lowercase, stripped)
    """
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    return df


# Group indicators by category
INDICATOR_CATEGORIES = {
    "All": [],
    "Trend": [],
    "Momentum": [],
    "Volume": [],
    "Volatility": [],
    "Others": []
}

for name, info in INDICATOR_HELP.items():
    category = info[0]
    if category in INDICATOR_CATEGORIES:
        INDICATOR_CATEGORIES[category].append(name)
        INDICATOR_CATEGORIES["All"].append(name)

# Sort each category alphabetically
for cat in INDICATOR_CATEGORIES:
    INDICATOR_CATEGORIES[cat] = sorted(INDICATOR_CATEGORIES[cat])


# --- Parameter Constraints for Indicators ---
# Define min/max/step values for each parameter type
PARAMETER_CONSTRAINTS = {
    # Window/period parameters (integers)
    "window": {"min": 2, "max": 500, "step": 1},
    "period": {"min": 2, "max": 500, "step": 1},
    "lbp": {"min": 2, "max": 100, "step": 1},

    # Step parameters (floats, for PSAR)
    "step": {"min": 0.01, "max": 0.5, "step": 0.01},
    "max_step": {"min": 0.05, "max": 0.5, "step": 0.01},

    # Window deviation for Bollinger/Keltner
    "window_dev": {"min": 0.5, "max": 4.0, "step": 0.1},

    # Multiple window parameters
    "window1": {"min": 2, "max": 500, "step": 1},
    "window2": {"min": 2, "max": 500, "step": 1},
    "window3": {"min": 2, "max": 500, "step": 1},
    "window_fast": {"min": 2, "max": 200, "step": 1},
    "window_slow": {"min": 2, "max": 200, "step": 1},
    "window_sign": {"min": 2, "max": 50, "step": 1},

    # Stochastic specific
    "smooth_window": {"min": 1, "max": 20, "step": 1},
}


def get_constraints(param_name: str) -> dict:
    """Get constraints for a parameter, with fallback to sensible defaults.

    Args:
        param_name: Name of the parameter

    Returns:
        Dict with min, max, step values
    """
    if param_name in PARAMETER_CONSTRAINTS:
        return PARAMETER_CONSTRAINTS[param_name]

    # Fallback: guess based on name
    if "window" in param_name.lower() or "period" in param_name.lower():
        return {"min": 2, "max": 500, "step": 1}
    elif "step" in param_name.lower():
        return {"min": 0.01, "max": 1.0, "step": 0.01}
    else:
        return {"min": 1, "max": 500, "step": 1}


def convert_param_value(param_name: str, value: float) -> int | float:
    """Convert a parameter value to the correct type (int or float).

    Args:
        param_name: Name of the parameter
        value: The value to convert

    Returns:
        int or float based on the parameter type
    """
    constraints = get_constraints(param_name)
    # If step is 1, it should be an integer
    if constraints.get("step") == 1:
        return int(value)
    return value


def get_engine():
    """Create SQLAlchemy engine for database connection."""
    conn_str = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(conn_str)


def get_known_tickers(engine) -> set[str]:
    """Get list of known tickers from database by querying existing tables.

    Args:
        engine: SQLAlchemy engine

    Returns:
        Set of valid ticker symbols (uppercase, with dots preserved)
    """
    try:
        with engine.connect() as conn:
            # Get all tables in the database
            query = text("""
                SELECT tablename
                FROM pg_tables
                WHERE schemaname = 'public'
                ORDER BY tablename
            """)
            result = conn.execute(query)
            # Convert table names back to ticker format (replace dashes with dots)
            tickers = {row[0].replace("-", ".").upper() for row in result if row[0]}
            return tickers
    except Exception as e:
        st.warning(f"Could not retrieve ticker list: {e}")
        return set()


def load_ohlcv(ticker: str, days: int = 365) -> pd.DataFrame:
    """Load OHLCV data from database.

    Args:
        ticker: Stock ticker symbol
        days: Number of days of history to load (0 = all history)

    Returns:
        DataFrame with OHLCV data indexed by Date
    """
    table = ticker.lower().replace(".", "-")
    engine = get_engine()

    try:
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
    finally:
        engine.dispose()

    return df


def prepare_candle_data(df: pd.DataFrame) -> list[CandlestickData]:
    """Convert DataFrame to candlestick data format.

    Args:
        df: DataFrame with OHLCV columns

    Returns:
        List of CandlestickData objects
    """
    df = normalize_columns(df)

    candle_data = []
    skipped_count = 0

    for idx, row in df.iterrows():
        if isinstance(idx, datetime.datetime):
            time_str = idx.strftime("%Y-%m-%d")
        else:
            time_str = str(idx)[:10]

        open_val = float(row["open"])
        high_val = float(row["high"])
        low_val = float(row["low"])
        close_val = float(row["close"])

        # Validate OHLC relationships
        # Valid candle: low <= open <= high AND low <= close <= high
        try:
            if not (low_val <= open_val <= high_val):
                skipped_count += 1
                continue
            if not (low_val <= close_val <= high_val):
                skipped_count += 1
                continue

            candle_data.append(CandlestickData(
                time=time_str,
                open=open_val,
                high=high_val,
                low=low_val,
                close=close_val,
            ))
        except (ValueError, TypeError):
            skipped_count += 1
            continue

    # Log if we skipped any invalid candles
    if skipped_count > 0:
        st.warning(
            f"Skipped {skipped_count} invalid candles due to data quality issues. "
            f"Showing {len(candle_data)} valid candles."
        )

    return candle_data


def prepare_volume_data(df: pd.DataFrame) -> list[HistogramData]:
    """Convert volume data to histogram format with colors.

    Args:
        df: DataFrame with OHLCV columns including volume

    Returns:
        List of HistogramData objects with color based on price direction
    """
    df = normalize_columns(df)

    volume_data = []
    skipped_count = 0

    for idx, row in df.iterrows():
        if isinstance(idx, datetime.datetime):
            time_str = idx.strftime("%Y-%m-%d")
        else:
            time_str = str(idx)[:10]

        try:
            close_val = float(row["close"])
            open_val = float(row["open"])
            volume_val = float(row["volume"])

            # Validate volume is non-negative
            if volume_val < 0:
                skipped_count += 1
                continue

            # Green for up day, red for down day
            color = "#26a69a" if close_val >= open_val else "#ef5350"

            volume_data.append(HistogramData(
                time=time_str,
                value=volume_val,
                color=color,
            ))
        except (ValueError, TypeError):
            skipped_count += 1
            continue

    return volume_data


def prepare_line_data(df: pd.DataFrame, indicator_series: pd.Series, color: str = "#2196f3") -> list[LineData]:
    """Convert indicator Series to LineData format for lightweight-charts.

    Args:
        df: DataFrame with OHLCV data (for index alignment)
        indicator_series: Series with indicator values
        color: Color for the line (optional)

    Returns:
        List of LineData objects
    """
    df = normalize_columns(df)

    line_data = []
    for idx, value in indicator_series.items():
        if pd.isna(value):
            continue

        if isinstance(idx, datetime.datetime):
            time_str = idx.strftime("%Y-%m-%d")
        else:
            time_str = str(idx)[:10]

        line_data.append(LineData(
            time=time_str,
            value=float(value),
            color=color,
        ))

    return line_data


def main():
    st.set_page_config(
        page_title="TradingView Style Charts",
        page_icon="📈",
        layout="wide"
    )

    st.title("📈 TradingView-Style Chart")

    # Sidebar controls
    st.sidebar.header("Chart Settings")

    # Ticker input with validation
    ticker_input = st.sidebar.text_input("Ticker", value="KLIC", key="ticker_input")
    ticker = ticker_input.strip().upper()

    # Validate ticker format
    if ticker and not is_valid_ticker(ticker):
        st.sidebar.error(f"Invalid ticker format: '{ticker}'. Use letters, numbers, and dots only (e.g., AAPL, BRK.B).")

    # Period selection
    period_options = {
        "30 Days": 30,
        "90 Days": 90,
        "180 Days": 180,
        "365 Days": 365,
        "2 Years": 730,
        "5 Years": 1825,
        "All History": 0,
    }
    selected_period = st.sidebar.selectbox(
        "Time Period",
        options=list(period_options.keys()),
        index=3,
        key="time_period"
    )
    days = period_options[selected_period]

    # --- Indicator Selection ---
    st.sidebar.header("Technical Indicators")

    # Category selection
    category_options = list(INDICATOR_CATEGORIES.keys())
    selected_category = st.sidebar.selectbox(
        "Indicator Category",
        options=category_options,
        index=0,
        key="ind_category"
    )

    # Get indicators for selected category
    category_indicators = INDICATOR_CATEGORIES[selected_category]

    # Create options with descriptions for the multi-select
    indicator_options = {
        name: f"{name} - {INDICATOR_HELP[name][1]}"
        for name in category_indicators
    }

    # Multi-select for indicators
    selected_indicators = st.sidebar.multiselect(
        "Select Indicators",
        options=category_indicators,
        format_func=lambda x: indicator_options.get(x, x) or str(x),
        key="ind_select"
    )

    # Mode selection for each selected indicator
    # Initialize session state for indicator modes if not exists
    if "indicator_modes" not in st.session_state:
        st.session_state.indicator_modes = {}

    # Initialize session state for indicator parameters if not exists
    if "indicator_params" not in st.session_state:
        st.session_state.indicator_params = {}

    # Clean up modes for indicators that are no longer selected
    for ind in list(st.session_state.indicator_modes.keys()):
        if ind not in selected_indicators:
            del st.session_state.indicator_modes[ind]

    # Clean up params for indicators that are no longer selected
    for ind in list(st.session_state.indicator_params.keys()):
        if ind not in selected_indicators:
            del st.session_state.indicator_params[ind]

    # Build mode selection for each selected indicator
    if selected_indicators:
        st.sidebar.markdown("**Display Mode:**")
        for ind in selected_indicators:
            is_overlay = ind in OVERLAY_INDICATORS
            default_mode = "overlay" if is_overlay else "separate"

            # Use existing mode from session state or set default
            if ind not in st.session_state.indicator_modes:
                st.session_state.indicator_modes[ind] = default_mode

            col1, col2 = st.sidebar.columns([2, 1])
            with col1:
                st.markdown(f"_{ind}_")
            with col2:
                st.session_state.indicator_modes[ind] = st.selectbox(
                    f"Mode {ind}",
                    options=["overlay", "separate"],
                    index=0 if st.session_state.indicator_modes[ind] == "overlay" else 1,
                    key=f"mode_{ind}",
                    label_visibility="collapsed"
                )

    # Parameter Configuration Section
    with st.sidebar.expander("⚙️ Configure Parameters", expanded=False):
        if not selected_indicators:
            st.info("👈 Please select indicators first to configure their parameters.")
        else:
            # Render parameter sections for each selected indicator
            for ind in selected_indicators:
                defaults = INDICATOR_HELP[ind][2]

                if not defaults:
                    continue  # Skip indicators with no parameters (e.g., OBV)

                # Indicator header with reset button
                col_a, col_b = st.sidebar.columns([4, 1])

                with col_a:
                    st.sidebar.markdown(f"**{ind}**")

                with col_b:
                    if st.sidebar.button("🔄", key=f"reset_{ind}", help="Reset to defaults"):
                        if ind in st.session_state.indicator_params:
                            del st.session_state.indicator_params[ind]
                        st.rerun()

                # Parameter inputs
                for param_name, default_value in defaults.items():
                    constraints = get_constraints(param_name)
                    current_value = st.session_state.indicator_params.get(ind, {}).get(param_name, default_value)

                    # Label with default and range info
                    label = param_name.replace("_", " ").title()
                    help_text = f"Default: {default_value} | Range: {constraints['min']}-{constraints['max']}"

                    new_value = st.sidebar.number_input(
                        label=label,
                        value=float(current_value),
                        min_value=float(constraints["min"]),
                        max_value=float(constraints["max"]),
                        step=float(constraints["step"]),
                        key=f"param_{ind}_{param_name}",
                        help=help_text
                    )

                    # Update session state
                    if ind not in st.session_state.indicator_params:
                        st.session_state.indicator_params[ind] = {}

                    # Only store if different from default (convert to proper type first)
                    converted_value = convert_param_value(param_name, new_value)
                    if converted_value != default_value:
                        st.session_state.indicator_params[ind][param_name] = converted_value
                    elif param_name in st.session_state.indicator_params[ind]:
                        del st.session_state.indicator_params[ind][param_name]

                st.sidebar.markdown("---")

            # Apply Changes button - triggers rerun but keeps expander open
            st.sidebar.button("Apply Changes", key="apply_params", type="primary")

    # Data Info (displayed after data is loaded, shown in sidebar)
    if "data_info" in st.session_state:
        st.sidebar.markdown("---")
        st.sidebar.markdown("**Data Info**")
        data = st.session_state.data_info
        st.sidebar.caption(f"Ticker: {data['ticker']}")
        st.sidebar.caption(f"Data Points: {data['data_points']}")
        st.sidebar.caption(f"Range: {data['date_start']} to {data['date_end']}")

    # Load data
    try:
        # Validate ticker before loading
        if not ticker:
            st.error("Please enter a ticker symbol.")
            return

        if not is_valid_ticker(ticker):
            st.error(f"Invalid ticker format: '{ticker}'. Use letters, numbers, and dots only.")
            return

        with st.spinner(f"Loading {ticker} data..."):
            # Optionally validate against known tickers
            engine = get_engine()
            known_tickers = get_known_tickers(engine)

            if known_tickers and ticker not in known_tickers:
                st.warning(
                    f"'{ticker}' is not in the known ticker list. "
                    f"Attempting to load anyway - the table may not exist."
                )

            df = load_ohlcv(ticker, days)

        if df.empty:
            st.error(f"No data found for {ticker}")
            return

        # Warn if loading very large dataset
        data_points = len(df)
        if data_points > 10000:
            st.warning(
                f"Loading {data_points} data points - this may cause performance issues. "
                f"Consider selecting a shorter time period."
            )
        st.success(f"Loaded {data_points} data points ({pd.to_datetime(df.index[0]).strftime('%Y-%m-%d')} to {pd.to_datetime(df.index[-1]).strftime('%Y-%m-%d')})")

        # Prepare chart data
        candle_data = prepare_candle_data(df)
        volume_data = prepare_volume_data(df)

        # Calculate indicators if any selected
        overlay_indicators = []  # (name, series, color)
        separate_indicators = []  # (name, series, color, pane_id)

        # Color palette for indicators
        indicator_colors = [
            "#ff9800", "#e91e63", "#9c27b0", "#00bcd4",
            "#8bc34a", "#ff5722", "#795548", "#607d8b"
        ]

        if selected_indicators and st.session_state.indicator_modes:
            # Normalize column names to lowercase for indicator calculation
            df_normalized = normalize_columns(df)

            for i, ind_name in enumerate(selected_indicators):
                try:
                    # Get user parameters (or empty dict for defaults)
                    user_params = st.session_state.indicator_params.get(ind_name, {})

                    # Calculate the indicator using normalized dataframe and user params
                    ind_series = calculate_indicator(df_normalized, ind_name, user_params)

                    mode = st.session_state.indicator_modes.get(ind_name, "separate")
                    color = indicator_colors[i % len(indicator_colors)]

                    # Skip if no valid data (all NaN)
                    if ind_series.notna().sum() == 0:
                        st.warning(
                            f"{ind_name}: Not enough data points for this indicator. "
                            f"Try a longer time period. Data points available: {len(df)}, "
                            f"Required: {INDICATOR_HELP[ind_name][2].get('window', 'varies')}"
                        )
                        continue

                    # Check if we have at least 50% valid data
                    valid_pct = (ind_series.notna().sum() / len(ind_series)) * 100
                    if valid_pct < 50:
                        st.warning(
                            f"{ind_name}: Only {valid_pct:.1f}% valid data. "
                            f"Consider a longer time period for more accurate results."
                        )

                    if mode == "overlay":
                        overlay_indicators.append((ind_name, ind_series, color))
                    else:
                        separate_indicators.append((ind_name, ind_series, color))
                except ValueError as ve:
                    # Missing required columns
                    st.error(
                        f"{ind_name}: {ve}. "
                        f"This indicator requires columns not present in your data."
                    )
                except KeyError as ke:
                    # Indicator not implemented
                    st.error(
                        f"{ind_name}: Indicator configuration error: {ke}. "
                        f"This indicator may not be available in the current version."
                    )
                except Exception as e:
                    # Other errors with specific details
                    st.error(
                        f"{ind_name}: Calculation failed - {type(e).__name__}: {e}. "
                        f"Try different parameters or a different indicator."
                    )

        # Create candlestick series
        candle_series = CandlestickSeries(data=candle_data)

        # Set colors after creation (type: ignore for Pyright warnings - these methods work at runtime)
        candle_series.set_up_color("#26a69a")  # type: ignore
        candle_series.set_down_color("#ef5350")  # type: ignore
        candle_series.set_border_up_color("#26a69a")  # type: ignore
        candle_series.set_border_down_color("#ef5350")  # type: ignore
        candle_series.set_wick_up_color("#26a69a")  # type: ignore
        candle_series.set_wick_down_color("#ef5350")  # type: ignore
        candle_series.set_border_visible(True)  # type: ignore

        # Create volume series in separate pane (pane_id=1)
        # pane_id layout: 0=price, 1=volume, 2+=indicators
        # Note: price_scale_id removed to allow crosshair to show volume values
        volume_series = HistogramSeries(
            data=volume_data,  # type: ignore
            pane_id=1,
        )

        # --- Chart Height Configuration ---
        # TradingView-style: taller charts with better pane height distribution
        # Configure minimum aspect ratio of 5:1 (y-axis is at least 20% of x-axis width)

        # Get approximate chart width (Streamlit wide layout is typically ~1920px on desktop)
        chart_width = 1920  # Default for wide layout

        # Calculate minimum height for 5:1 aspect ratio (height >= width * 0.20)
        min_indicator_height = int(chart_width * 0.20)  # ~384px

        # Base configuration
        pane_heights = {}

        # Calculate factors based on minimum height requirements
        # Price: ~630px, Volume: ~135px (from original 900px base)
        # Each indicator: ~960px (for 1:2 aspect ratio)

        price_height = 630
        volume_height = 135

        if separate_indicators:
            # Total required height = price + volume + (num_indicators * min_indicator_height)
            total_required_height = price_height + volume_height + (len(separate_indicators) * min_indicator_height)
            base_height = total_required_height

            # Calculate factors for each pane
            pane_heights[0] = PaneHeightOptions(factor=price_height / total_required_height)
            pane_heights[1] = PaneHeightOptions(factor=volume_height / total_required_height)

            # Each indicator gets equal factor that results in min_indicator_height
            indicator_factor = min_indicator_height / total_required_height
            for i in range(len(separate_indicators)):
                pane_heights[2 + i] = PaneHeightOptions(factor=indicator_factor)
        else:
            # No indicators: use original configuration
            base_height = 900
            pane_heights[0] = PaneHeightOptions(factor=0.70)  # Main price chart
            pane_heights[1] = PaneHeightOptions(factor=0.15)  # Volume

        # Create layout options with pane heights
        layout_options = LayoutOptions(
            pane_heights=pane_heights
        )

        # Create chart options with height and crosshair enabled
        # Note: Removed price_scale_id from series to allow crosshair to show values for all panes
        chart_options = ChartOptions(
            height=base_height,
            layout=layout_options,
            auto_size=False,  # Disable auto-size to use our configured height
        )

        # Build chart with both series
        try:
            chart = Chart(series=candle_series, options=chart_options)
            chart.add_series(volume_series)

            # Add overlay indicators to main pane (pane_id=0)
            for ind_name, ind_series, color in overlay_indicators:
                line_data = prepare_line_data(df, ind_series, color)
                if line_data:
                    line_series = LineSeries(data=line_data, pane_id=0)
                    chart.add_series(line_series)

            # Add separate panel indicators (pane_id starts at 2)
            for i, (ind_name, ind_series, color) in enumerate(separate_indicators):
                line_data = prepare_line_data(df, ind_series, color)
                if line_data:
                    # pane_id starts at 2 (1 is already used by volume)
                    pane_id = 2 + i
                    # Note: price_scale_id removed to allow crosshair to show indicator values
                    line_series = LineSeries(
                        data=line_data,
                        pane_id=pane_id,
                    )
                    chart.add_series(line_series)
        except Exception as chart_error:
            # If there's any error building chart with indicators, rebuild without them
            st.warning(
                f"Could not add indicators to chart ({type(chart_error).__name__}: {chart_error}). "
                f"Displaying price and volume only."
            )
            chart = Chart(series=candle_series, options=chart_options)
            chart.add_series(volume_series)

        # Render chart
        if overlay_indicators or separate_indicators:
            indicator_names = [ind[0] for ind in overlay_indicators] + [ind[0] for ind in separate_indicators]
            st.markdown(f"### {ticker} — Candlestick + {', '.join(indicator_names)}")
        else:
            st.markdown("### Candlestick Chart with Volume")

        # Create unique key based on ticker, period, indicators, and their parameters to force re-render
        indicators_key = "_".join(sorted(selected_indicators))

        # Add parameter values to chart key to force re-render when params change
        params_key = ""
        if selected_indicators and "indicator_params" in st.session_state:
            for ind in sorted(selected_indicators):
                if ind in st.session_state.indicator_params and st.session_state.indicator_params[ind]:
                    params = st.session_state.indicator_params[ind]
                    # Convert params to a stable string representation
                    param_str = "_".join(f"{k}={v}" for k, v in sorted(params.items()))
                    params_key += f"_{ind}_{param_str}"

        chart_key = f"chart_{ticker}_{days}_{indicators_key}{params_key}"

        try:
            chart.render(key=chart_key)
        except Exception as render_error:
            st.error(f"Chart rendering error ({type(render_error).__name__}): {render_error}")
            import traceback
            with st.expander("Show rendering error details"):
                st.code(traceback.format_exc())

        # Store data info in session state for sidebar display
        st.session_state.data_info = {
            "ticker": ticker,
            "data_points": len(df),
            "date_start": pd.to_datetime(df.index[0]).strftime('%Y-%m-%d'),
            "date_end": pd.to_datetime(df.index[-1]).strftime('%Y-%m-%d')
        }

    except Exception as e:
        st.error(f"Error loading data: {type(e).__name__}: {e}")
        import traceback
        with st.expander("Show error details"):
            st.code(traceback.format_exc())


if __name__ == "__main__":
    main()