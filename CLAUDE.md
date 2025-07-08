# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Running the Application
```bash
# Interactive mode (recommended) - provides menu-driven interface
python main.py

# Command-line mode examples
python main.py download --symbol BTCUSDT --type spot --granularity daily --start 2024-01-01 --end 2024-01-31
python main.py optimize --source datasets/dataset-raw-daily-compressed --target datasets/dataset-raw-daily-compressed-optimized --max-size 10
python main.py validate --data-dir datasets/dataset-raw-daily-compressed-optimized/spot --symbol BTCUSDT
python main.py features --type imbalance
```

### Dependencies
```bash
# Install dependencies
pip install -r requirements.txt
```

### Jupyter Notebooks
```bash
# Run Jupyter notebooks
jupyter notebook
```

## Architecture

This is a Bitcoin/cryptocurrency data pipeline for machine learning finance applications. The system follows a modular ETL architecture:

### Data Flow
1. **Download** → Binance historical data (ZIP files with checksums)
2. **Extract** → ZIP to CSV conversion with validation
3. **Convert** → CSV to Parquet format for efficiency
4. **Optimize** → Merge and optimize Parquet files (configurable size limits)
5. **Validate** → Check data integrity and missing dates
6. **Features** → Generate advanced features (imbalance dollar bars using Dask/Numba)

### Key Components
- **main.py**: Central orchestrator with interactive CLI and command-line interface
- **src/data_pipeline/**: Core ETL modules (downloaders, extractors, converters, processors, validators)
- **src/features/**: Feature engineering modules (imbalance_bars.py)
- **datasets/**: Data storage with progress tracking (JSON files)
- **output/**: Generated features and processed data

### Design Principles
- **Integrity First**: Every step includes validation
- **Resume Capability**: Progress tracking allows resuming interrupted operations
- **Memory Efficient**: Uses Dask for distributed processing of large datasets
- **Performance Optimized**: Numba JIT compilation for compute-intensive operations
- **Modular**: Each pipeline step is independent and can be run separately

### Data Types Supported
- Markets: spot, futures (USD-M), futures (COIN-M)
- Granularities: daily, monthly
- Formats: ZIP → CSV → Parquet → Optimized Parquet

### Progress Tracking
Progress is saved in JSON files (e.g., `spot_progress_BTCUSDT.json`) allowing operations to resume from interruption points.

### Important Patterns
- Use interactive mode for exploring options
- Always validate data after optimization
- Check logs in `datasets/logs/` for detailed processing information
- Parquet files are auto-cleaned after successful conversion
- Missing dates validator can check both file-level and daily gaps