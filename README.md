# Crypto ML Finance Pipeline

A high-performance Python pipeline for downloading and processing cryptocurrency data into Parquet files for machine learning.

## Quick Start

### 1. Install Dependencies

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install requirements
pip install -r requirements.txt
```

### 2. Run the Pipeline

```bash
# Interactive mode (recommended)
python main.py
```

### 3. Command Line Usage

```bash
# Download data
python main.py download --symbol BTCUSDT --type spot --granularity daily

# Optimize parquet files
python main.py optimize --source data/raw --target data/optimized

# Validate data
python main.py validate --symbol BTCUSDT

# Generate features (Imbalance Dollar Bars)
python main.py features --type imbalance
```

## Data Structure

```
data/
├── raw-daily/                 # Downloaded ZIP files
├── raw-daily-compressed/       # Converted Parquet files
├── raw-daily-compressed-optimized/  # Optimized Parquet files
└── logs/                       # Processing logs
```

## Features

- **Download** - Historical crypto data from Binance
- **Convert** - ZIP → CSV → Parquet format
- **Optimize** - Merge and compress Parquet files
- **Validate** - Check for missing dates
- **Generate** - Imbalance Dollar Bars (Lopez de Prado method)

## Working with Parquet Files

```python
import pandas as pd

# Read single file
df = pd.read_parquet("data/BTCUSDT_2024.parquet")

# Read multiple files
import dask.dataframe as dd
df = dd.read_parquet("data/*.parquet")
```

## Requirements

- Python 3.8+
- ~10GB disk space per year of data (Parquet format)
- Internet connection for downloading from Binance

## Documentation

📚 **Complete documentation available in [`documentation/`](documentation/)**

- [INDEX.md](documentation/INDEX.md) - Documentation index
- [DATA_STRUCTURE.md](documentation/DATA_STRUCTURE.md) - Directory structure and organization
- [CPU_OPTIMIZATION.md](documentation/CPU_OPTIMIZATION.md) - Performance optimizations (6x faster!)
- [CHECKSUM_VERIFICATION.md](documentation/CHECKSUM_VERIFICATION.md) - Data integrity verification
- [PARQUET_COMPRESSION.md](documentation/PARQUET_COMPRESSION.md) - Compression details

See [CLAUDE.md](CLAUDE.md) for architecture overview and design principles.

## License

MIT