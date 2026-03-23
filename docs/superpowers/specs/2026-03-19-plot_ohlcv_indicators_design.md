# plot_ohlcv.py Indicator Enhancement Design

## Overview

Enhance `plot_ohlcv.py` to support plotting any technical indicator from the `ta` Python library with either overlay or separate row display modes, controlled via command-line arguments.

## Command-Line Interface

```bash
python plot_ohlcv.py TICKER [OPTIONS]

Options:
  --period DAYS     Number of calendar days to plot (default: 365, 0 = all)
  --indicator IND   Add indicator (can be specified multiple times)
                    Format: NAME:MODE[:PARAM=VALUE,...]
  --help-indicators Display all available indicators with descriptions
  --indicator-help NAME Show help for specific indicator
```

### Indicator Argument Format

```
NAME:MODE[:PARAM=VALUE,...]
```

Examples:
- `RSI:separate` — RSI with defaults (period=14)
- `SMA:overlay:period=50` — 50-period SMA on price chart
- `MACD:separate:fast=12,slow=26,signal=9` — custom MACD params
- Multiple: `--indicator RSI:separate --indicator SMA:overlay:period=200`

## Display Modes

- **overlay**: Plotted on the main price chart (Row 1)
- **separate**: Gets its own dedicated row below price and volume

## Plot Layout

- Row 1: Candlestick (price) + overlay indicators
- Row 2: Volume bars
- Row 3+: Each `separate` indicator gets its own row

Row heights:
- Price row: 50% of available space
- Volume row: 20%
- Each separate indicator: minimum 15%, divided equally among all separate indicators

## Implementation

### 1. Indicator Registry with Docstrings

Create callable indicator functions with full docstrings for IDE hover support in Zed:

```python
def SMA(close: pd.Series, period: int = 20) -> pd.Series:
    """Simple Moving Average (SMA)

    A simple moving average that smooths price data over a specified period.

    Args:
        close: Close prices Series
        period: Number of periods (default: 20)

    Returns:
        SMA values as Series
    """
    return SMAIndicator(close=close, window=period).sma_indicator()
```

This pattern applies to ALL indicators from the ta library.

### 2. Indicator Help System

- `--help-indicators`: Lists all available indicators organized by category with one-line descriptions
- `--indicator-help NAME`: Shows detailed help for a specific indicator including all parameters

### 3. Indicator Parsing

Parse `--indicator` argument:
1. Split by `:` to get NAME, MODE, and optional params
2. Validate MODE is `overlay` or `separate`
3. Parse optional PARAM=VALUE pairs
4. Map NAME to corresponding indicator function
5. Apply user params or defaults

### 4. Dynamic Subplot Creation

Calculate number of rows based on:
- 1 row for candlestick
- 1 row for volume
- N rows for each `separate` indicator

Use `make_subplots` with dynamic row count.

### 5. Data Requirements Mapping

Different indicators require different OHLCV columns:

| Columns Needed | Indicators |
|----------------|------------|
| close only | SMA, EMA, WMA, KAMA, RSI, MACD, etc. |
| close, volume | OBV, VWAP, MFI, ForceIndex, etc. |
| high, low, close | ATR, Bollinger Bands, Keltner, CCI, etc. |
| high, low, close, volume | ADI, CMF, VWMA, etc. |

The implementation will pass required columns to each indicator function.

## Available Indicators (from ta library)

### Trend (overlay-friendly)
- SMA, EMA, WMA, KAMA — Moving averages
- TRIX — Triple EMA momentum
- MACD, MACD_signal, MACD_diff — MACD family
- ADX, ADX_pos, ADX_neg — Directional Movement
- Aroon, Aroon_up, Aroon_down — Aroon indicators
- CCI — Commodity Channel Index
- DPO — Detrended Price Oscillator
- KST, KST_sig — Know Sure Thing
- MassIndex — Mass Index
- Vortex — Vortex Indicator
- STC — Schaff Trend Cycle
- PSAR — Parabolic SAR
- Ichimoku_conv, Ichimoku_base, Ichimoku_a, Ichimoku_b — Ichimoku Cloud

### Momentum (separate recommended)
- RSI — Relative Strength Index
- TSI — True Strength Index
- Stochastic, Stochastic_signal — Stochastic Oscillator
- Williams_R — Williams %R
- AwesomeOsc — Awesome Oscillator
- ROC — Rate of Change
- StochRSI, StochRSI_k, StochRSI_d — Stochastic RSI
- UltimateOsc — Ultimate Oscillator
- PPO, PPO_signal, PPO_hist — Percentage Price Oscillator

### Volume
- OBV — On-Balance Volume
- MFI — Money Flow Index
- CMF — Chaikin Money Flow
- VWAP — Volume Weighted Average Price
- ADI — Accumulation/Distribution
- ForceIndex — Force Index
- VPT — Volume Price Trend
- NVI — Negative Volume Index
- EaseOfMovement — Ease of Movement
- PVO — Percentage Volume Oscillator

### Volatility
- ATR — Average True Range
- Bollinger_mavg, Bollinger_hband, Bollinger_lband, Bollinger_wband, Bollinger_pband — Bollinger Bands
- Keltner_mband, Keltner_hband, Keltner_lband — Keltner Channels
- Donchian_hband, Donchian_lband, Donchian_mband — Donchian Channel
- UlcerIndex — Ulcer Index

### Others
- DailyReturn, DailyLogReturn, CumulativeReturn

## Default Parameters

Common defaults (used when not specified):
- `period`, `window`: 14 (20 for some moving averages)
- `fast`: 12
- `slow`: 26
- `signal`: 9

## Error Handling

- Invalid indicator name: Show error with suggestion for similar names
- Invalid parameter: Show error listing valid parameters
- Missing required data: Show error indicating which columns are needed
- Indicator calculation error: Show error with details

## Backwards Compatibility

- If no `--indicator` specified, behavior is identical to original: just OHLCV + volume
- Default ticker remains "KLIC"
- Default period remains 365 days

## Files Modified

- `plot_ohlcv.py` — Main file enhanced with indicator support

## Testing

Test cases:
1. No indicators (original behavior)
2. Single overlay indicator
3. Single separate indicator
4. Multiple overlay + separate indicators
5. Custom parameters
6. Invalid indicator name
7. Invalid parameter
8. --help-indicators flag
9. --indicator-help flag