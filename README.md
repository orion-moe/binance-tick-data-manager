# Binance Tick Data Manager

High-performance pipeline for downloading and processing cryptocurrency data into Parquet files for machine learning.

## Quick Start

### 1. Installation

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Run Pipeline

```bash
# Interactive mode (recommended)
python main.py
```

## Pipeline Steps

The pipeline runs in sequential steps:

### Step 1: Download
- Download historical ZIPs from Binance
- Automatic checksum verification
- Support for Spot and Futures (USD-M / COIN-M)
- Progress saved in `download_progress_daily.json`

### Step 2: Conversion
Two options available:
- **Legacy**: ZIP → CSV → Parquet (slower, uses more disk)
- **Optimized**: ZIP → Parquet direct (streaming, recommended)

### Step 3: Merge/Optimization
- Groups daily Parquet files into larger files (~10GB)
- Snappy compression for better performance
- Automatic cleanup of intermediate files

### Step 4: Validation
- Missing dates verification
- Parquet file integrity validation
- Corrupted file detection

### Step 5: Features
Alternative bar generation for ML:
- **Standard Dollar Bars**: Bars by fixed dollar volume
- **Imbalance Dollar Bars**: Adaptive bars based on imbalance

## Directory Structure

```
binance-tick-data-manager/
├── main.py                    # Main entry point
├── src/
│   ├── data_pipeline/         # ETL modules
│   │   ├── downloaders/       # Binance download
│   │   ├── extractors/        # CSV extraction
│   │   ├── converters/        # Parquet conversion
│   │   ├── processors/        # Merge and optimization
│   │   ├── validators/        # Data validation
│   │   └── utils/             # Utilities
│   ├── features/              # Feature engineering
│   │   └── bars/              # Bar generation
│   └── scripts/               # Helper scripts
├── data/                      # Data (per ticker)
│   ├── btcusdt-spot/
│   │   ├── raw-zip-daily/            # Downloaded ZIPs
│   │   ├── raw-parquet-daily/        # Individual Parquets
│   │   ├── raw-parquet-merged-daily/ # Merged Parquets (~10GB)
│   │   ├── output/                   # Generated features
│   │   └── logs/                     # Local logs
│   ├── btcusdt-futures-um/           # Futures USD-M
│   └── logs/                         # Global logs
├── output/                    # Features (organized by ticker)
└── notebooks/                 # Exploratory analysis
```

## Implemented Features

| Feature | Status | Description |
|---------|--------|-------------|
| Download with checksum | ✅ | Automatic SHA256 verification |
| Spot support | ✅ | Spot market data |
| Futures support | ✅ | USD-M and COIN-M |
| ZIP → Parquet streaming | ✅ | Optimized conversion without intermediate CSV |
| Parquet merge | ✅ | Groups into ~10GB files |
| Date validation | ✅ | Detects data gaps |
| Standard Dollar Bars | ✅ | Bars by fixed dollar volume |
| Imbalance Dollar Bars | ✅ | Adaptive bars by imbalance |
| Progress tracking | ✅ | Resumes interrupted downloads |

## In Development

| Feature | Status | Description |
|---------|--------|-------------|
| Imbalance Bars (tick) | 🔄 | Bars by tick imbalance |
| Unit tests | ⬜ | Automated test suite |
| Tick Bars | ⬜ | Bars by tick count |
| Volume Bars | ⬜ | Bars by contract volume |
| CLI arguments | ⬜ | Command-line execution |
| ML models | ⬜ | ML framework integration |

## Data Usage

### Reading Parquet

```python
import pandas as pd

# Single file
df = pd.read_parquet("data/btcusdt-spot/raw-parquet-merged-daily/merged_part_0.parquet")

# Multiple files with Dask (files larger than RAM)
import dask.dataframe as dd
df = dd.read_parquet("data/btcusdt-spot/raw-parquet-merged-daily/*.parquet")
```

### Reading Dollar Bars

```python
import pandas as pd

# Standard Dollar Bars
df = pd.read_parquet("data/btcusdt-spot/output/standard_dollar_bars.parquet")

# Imbalance Dollar Bars
df = pd.read_parquet("output/btcusdt-spot/imbalance_dollar_bars.parquet")
```

## Requirements

- Python 3.8+
- ~10GB disk space per year of data
- Internet connection for Binance download

## Architecture

### Design Principles

- **Simple**: No Docker, databases, or complex infrastructure
- **Fast**: Parquet files for 10x better performance than CSV
- **Reliable**: Checksum verification and data validation
- **Resumable**: Progress tracking for interrupted operations
- **Organized**: Each ticker in its own directory

### Technologies

- **PyArrow**: Parquet read/write
- **Dask**: Processing files larger than RAM
- **Pandas**: DataFrame manipulation
- **httpx**: Async HTTP downloads

## License

MIT
