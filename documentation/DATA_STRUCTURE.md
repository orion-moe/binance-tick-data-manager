# Data Directory Structure

This directory contains all trading data organized by ticker and market type.

## Organization

Each ticker gets its own directory following this pattern:
```
{symbol}-{market_type}/
```

Examples:
- `btcusdt-spot/` - BTCUSDT spot market
- `btcusdt-futures-um/` - BTCUSDT USD-M futures
- `ethusdt-spot/` - ETHUSDT spot market

## Subdirectory Structure

Inside each ticker directory:

```
btcusdt-spot/
├── raw-zip-daily/              # Downloaded ZIP/CSV files
├── raw-parquet-daily/          # Processed Parquet files (1:1 from ZIPs, snappy compressed)
├── raw-parquet-merged-daily/         # Large merged Parquet files (~10GB each, snappy compressed)
├── logs/                       # Download and processing logs
├── download_progress_daily.json    # Progress tracking for downloads
└── failed_downloads.txt        # List of failed download attempts
```

## Directory Names Explained

### Raw Data (Input)
- **`raw-zip-daily/`** - Original ZIP files downloaded from Binance

### Processed Data (Intermediate)
- **`raw-parquet-daily/`** - ZIP files converted to Parquet format (one file per day, snappy compressed)

### Optimized Data (Output)
- **`raw-parquet-merged-daily/`** - Parquet files merged and optimized into ~10GB files
  - Combines multiple daily files
  - Uses Snappy compression (fast read/write)
  - Optimized row group size for ML workloads

## Parquet Compression

All Parquet files use **Snappy compression**:
- ✅ **Fast**: Best balance between compression ratio and speed
- ✅ **Reliable**: Industry standard, widely supported
- ✅ **Consistent**: Same compression used throughout the pipeline

**Once set, compression is automatically maintained** - you don't need to specify it again!

### Why Snappy?

| Compression | Speed | Ratio | Use Case |
|-------------|-------|-------|----------|
| **Snappy** | ⚡⚡⚡ Fast | ~2-3x | **ML/Analytics** ← We use this |
| GZIP | 🐌 Slow | ~5-10x | Long-term storage |
| LZ4 | ⚡⚡⚡ Fastest | ~1.5-2x | Real-time streaming |
| Zstd | ⚡⚡ Medium | ~3-7x | General purpose |

**Snappy is optimal for:**
- Fast data loading during training
- Quick exploratory analysis
- High-throughput ML pipelines

## Git Protection

All files in this directory are automatically ignored by git via `.gitignore`:
- ✅ No raw data will ever be committed
- ✅ No progress files will be committed
- ✅ Only the directory structure is tracked

## Usage

When you run:
```bash
python main.py download --symbol BTCUSDT --type spot --granularity daily
```

Data will be stored in:
```
data/btcusdt-spot/raw-zip-daily/       # Downloaded ZIP files
data/btcusdt-spot/raw-parquet-daily/   # Processed parquet files (snappy)
data/btcusdt-spot/logs/                # Download logs
```

## Pipeline Flow

```
┌──────────────────────────────────────────────────────┐
│ 1. Download                                          │
│    raw-zip-daily/ (100-200MB per file)              │
└──────────────────────────────────────────────────────┘
                        ↓
┌──────────────────────────────────────────────────────┐
│ 2. Convert to Parquet (snappy compression)           │
│    raw-parquet-daily/ (50-100MB per file)            │
│    • 1 parquet per day                               │
│    • Snappy compression applied automatically        │
└──────────────────────────────────────────────────────┘
                        ↓
┌──────────────────────────────────────────────────────┐
│ 3. Merge & Optimize (snappy compression maintained)  │
│    raw-parquet-merged-daily/ (~10GB per file)              │
│    • Combines multiple files                         │
│    • Same snappy compression                         │
│    • Optimized for ML workloads                      │
└──────────────────────────────────────────────────────┘
```

## Benefits of This Structure

1. **Clear Naming**: Easy to understand what each folder contains
   - `raw-zip-daily/` = original downloads
   - `raw-parquet-daily/` = converted with snappy compression
   - `raw-parquet-merged-daily/` = final merged files (snappy)

2. **Isolated**: Each ticker's data is completely separated

3. **Scalable**: Easy to add new tickers without affecting existing ones

4. **Safe**: Git will never track your data files

5. **Optimized**: Snappy compression throughout for best ML performance

6. **Automatic**: Compression is set once and maintained automatically

## Example: BTCUSDT Spot Data

After downloading one year of daily data:

```
data/btcusdt-spot/
├── raw-zip-daily/
│   ├── BTCUSDT-trades-2024-01-01.zip (200MB)
│   ├── BTCUSDT-trades-2024-01-02.zip (200MB)
│   └── ... (365 files)
│
├── raw-parquet-daily/
│   ├── BTCUSDT-Trades-2024-01-01.parquet (65MB, snappy)
│   ├── BTCUSDT-Trades-2024-01-02.parquet (65MB, snappy)
│   └── ... (365 files)
│
└── raw-parquet-merged-daily/
    ├── BTCUSDT-Trades-Optimized-001.parquet (10GB, snappy)
    ├── BTCUSDT-Trades-Optimized-002.parquet (10GB, snappy)
    └── BTCUSDT-Trades-Optimized-003.parquet (4GB, snappy)
```

**Result**: 365 days compressed into 3 optimized files ready for ML!
