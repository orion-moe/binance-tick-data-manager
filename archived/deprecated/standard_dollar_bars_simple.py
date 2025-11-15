#!/usr/bin/env python3
"""
SIMPLIFIED Standard Dollar Bars - WITHOUT Dask
Uses only Pandas + Numba for maximum performance and stability
"""

import os
import logging
import pandas as pd
import numpy as np
import glob
from numba import njit, prange
from pathlib import Path
import gc
import time
from tqdm import tqdm

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_data_path(data_type='futures', futures_type='um', granularity='daily'):
    """Builds the path to the data."""
    project_root = Path(__file__).resolve().parent.parent.parent
    data_dir = project_root / 'data'

    if data_type == 'spot':
        return data_dir / f'dataset-raw-{granularity}-compressed-optimized' / 'spot'
    else:
        return data_dir / f'dataset-raw-{granularity}-compressed-optimized' / f'futures-{futures_type}'

# ==============================================================================
# ALGORITHM CORE - Numba JIT Compiled
# ==============================================================================

@njit(cache=True, fastmath=True)
def generate_dollar_bars_numba(prices, times, volumes_usd, sides, qtys, threshold):
    """
    Generates dollar bars optimized with Numba.

    Args:
        prices: Price array
        times: Timestamp array
        volumes_usd: Volume array in USD (qty * price)
        sides: Array indicating transaction side (-1 sell, 1 buy)
        qtys: Quantity array
        threshold: Volume threshold in USD to form a bar

    Returns:
        List of bars
    """
    n = len(prices)
    bars = []

    # Current bar state
    bar_start_idx = 0
    accumulated_volume = 0.0
    buy_volume = 0.0
    sell_volume = 0.0

    for i in range(n):
        # Accumulate volume
        trade_volume = volumes_usd[i]
        accumulated_volume += trade_volume

        if sides[i] == 1:  # Buy
            buy_volume += trade_volume
        else:  # Sell
            sell_volume += trade_volume

        # Check if should form a bar
        if accumulated_volume >= threshold:
            # Extract bar data
            bar_prices = prices[bar_start_idx:i+1]

            bar_open = prices[bar_start_idx]
            bar_close = prices[i]
            bar_high = np.max(bar_prices)
            bar_low = np.min(bar_prices)
            bar_volume = accumulated_volume
            bar_trades = i - bar_start_idx + 1

            # Calculate imbalance
            imbalance = (buy_volume - sell_volume) / accumulated_volume if accumulated_volume > 0 else 0

            bars.append([
                float(times[bar_start_idx]),  # start_time
                float(times[i]),               # end_time
                float(bar_open),               # open
                float(bar_high),               # high
                float(bar_low),                # low
                float(bar_close),              # close
                float(bar_volume),             # volume_usd
                float(buy_volume),             # buy_volume_usd
                float(sell_volume),            # sell_volume_usd
                float(imbalance),              # imbalance
                int(bar_trades),               # num_trades
                float(np.sum(qtys[bar_start_idx:i+1]))  # total_qty
            ])

            # Reset for next bar
            bar_start_idx = i + 1
            accumulated_volume = 0.0
            buy_volume = 0.0
            sell_volume = 0.0

    return bars

# ==============================================================================
# MAIN PROCESSING
# ==============================================================================

def process_files_simple(data_path, threshold=40_000_000):
    """
    Processes files in a simple and efficient way.
    """
    # List all files
    pattern = str(data_path / "*.parquet")
    files = sorted(glob.glob(pattern))

    if not files:
        logging.error(f"❌ No files found in: {data_path}")
        return pd.DataFrame()

    logging.info(f"📁 Found {len(files)} files to process")

    all_bars = []
    total_rows = 0

    # Process file by file
    for file_idx, file_path in enumerate(tqdm(files, desc="Processing files")):
        try:
            # Read the file
            df = pd.read_parquet(
                file_path,
                columns=['time', 'price', 'qty', 'quoteQty', 'isBuyerMaker'],
                engine='pyarrow'
            )

            if df.empty:
                continue

            # Pre-processing
            df['side'] = np.where(df['isBuyerMaker'], -1, 1).astype(np.int8)
            df['volume_usd'] = df['quoteQty'].astype(np.float64)

            # Convert to NumPy arrays (more efficient)
            prices = df['price'].values.astype(np.float64)
            # Handle time column: convert to milliseconds since epoch if it's datetime
            if df['time'].dtype == 'datetime64[ns]' or df['time'].dtype == 'datetime64[ms]':
                times = df['time'].astype(np.int64) / 1e6  # Convert nanoseconds to milliseconds
            else:
                times = df['time'].values.astype(np.float64)
            volumes_usd = df['volume_usd'].values
            sides = df['side'].values
            qtys = df['qty'].values.astype(np.float64)

            # Generate bars with Numba
            bars = generate_dollar_bars_numba(
                prices, times, volumes_usd, sides, qtys, threshold
            )

            if bars:
                all_bars.extend(bars)
                logging.info(f"   File {file_idx+1}/{len(files)}: {len(bars)} bars generated")

            total_rows += len(df)

            # Free memory
            del df, prices, times, volumes_usd, sides, qtys
            gc.collect()

        except Exception as e:
            logging.error(f"❌ Error processing {file_path}: {e}")
            continue

    # Convert to DataFrame
    if all_bars:
        df_bars = pd.DataFrame(all_bars, columns=[
            'start_time', 'end_time', 'open', 'high', 'low', 'close',
            'volume_usd', 'buy_volume_usd', 'sell_volume_usd', 'imbalance',
            'num_trades', 'total_qty'
        ])

        # Convert timestamps - handle both milliseconds and already datetime formats
        try:
            df_bars['start_time'] = pd.to_datetime(df_bars['start_time'], unit='ms')
            df_bars['end_time'] = pd.to_datetime(df_bars['end_time'], unit='ms')
        except Exception as e:
            # If conversion fails, try without unit specification (might already be datetime)
            logging.warning(f"Timestamp conversion with unit='ms' failed: {e}. Trying alternative conversion.")
            df_bars['start_time'] = pd.to_datetime(df_bars['start_time'])
            df_bars['end_time'] = pd.to_datetime(df_bars['end_time'])

        logging.info(f"✅ Total: {len(df_bars)} bars generated from {total_rows:,} trades")
        return df_bars
    else:
        logging.warning("⚠️ No bars were generated")
        return pd.DataFrame()

# ==============================================================================
# MAIN FUNCTION
# ==============================================================================

def generate_standard_bars_simple(
    data_type='spot',
    futures_type='um',
    granularity='daily',
    threshold=40_000_000,
    output_dir='./output/standard/'
):
    """
    Generates standard dollar bars in a simple and stable way.

    Args:
        data_type: 'spot' or 'futures'
        futures_type: 'um' or 'cm' (only for futures)
        granularity: 'daily' or 'monthly'
        threshold: Volume threshold in USD
        output_dir: Output directory

    Returns:
        DataFrame with generated bars
    """
    # Setup
    data_path = get_data_path(data_type, futures_type, granularity)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    logging.info("="*60)
    logging.info("🎯 STANDARD DOLLAR BARS - SIMPLIFIED VERSION")
    logging.info("="*60)
    logging.info(f"📁 Data path: {data_path}")
    logging.info(f"💰 Threshold: ${threshold:,.0f}")
    logging.info(f"📊 Type: {data_type.upper()}")

    # Check if directory exists
    if not data_path.exists():
        logging.error(f"❌ Directory not found: {data_path}")
        return pd.DataFrame()

    # Time measurement
    start_time = time.time()

    # Process files
    df_bars = process_files_simple(data_path, threshold)

    # Elapsed time
    elapsed_time = time.time() - start_time

    if not df_bars.empty:
        # Add statistics
        df_bars['duration_seconds'] = (df_bars['end_time'] - df_bars['start_time']).dt.total_seconds()
        df_bars['avg_price'] = (df_bars['open'] + df_bars['close']) / 2
        df_bars['price_range'] = df_bars['high'] - df_bars['low']
        df_bars['returns'] = df_bars['close'].pct_change()

        # Save result
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f"dollar_bars_{data_type}_{threshold}_{timestamp}.parquet"
        df_bars.to_parquet(output_file, engine='pyarrow', compression='snappy')

        logging.info("="*60)
        logging.info(f"✅ PROCESSING COMPLETED!")
        logging.info(f"⏱️ Total time: {elapsed_time:.2f} seconds")
        logging.info(f"📊 Total bars: {len(df_bars):,}")
        logging.info(f"💾 File saved: {output_file}")
        logging.info(f"📈 Period: {df_bars['start_time'].min()} to {df_bars['end_time'].max()}")
        logging.info("="*60)

    return df_bars

# ==============================================================================
# DIRECT TEST
# ==============================================================================

if __name__ == "__main__":
    import sys

    # Default configuration
    config = {
        'data_type': 'spot',
        'futures_type': 'um',
        'granularity': 'daily',
        'threshold': 40_000_000
    }

    # Parse simple arguments
    if len(sys.argv) > 1:
        if sys.argv[1] in ['spot', 'futures']:
            config['data_type'] = sys.argv[1]

    if len(sys.argv) > 2:
        config['threshold'] = float(sys.argv[2])

    print(f"Running with configuration: {config}")

    # Execute
    df = generate_standard_bars_simple(**config)

    if not df.empty:
        print("\n📊 First 5 bars:")
        print(df.head())
        print("\n📊 Statistics:")
        print(df.describe())