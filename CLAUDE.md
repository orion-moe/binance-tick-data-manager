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

# Core dependencies: arcticdb, pandas, pyarrow
```

### Jupyter Notebooks
```bash
# Run Jupyter notebooks
jupyter notebook

# Available notebook: global_analysis.ipynb
```

## Architecture

This is a Bitcoin/cryptocurrency data pipeline for machine learning finance applications. The system follows a modular ETL architecture with integrity-first design principles.

### Data Flow
1. **Download** → Binance historical data (ZIP files with checksums)
2. **Extract** → ZIP to CSV conversion with validation
3. **Convert** → CSV to Parquet format for efficiency
4. **Optimize** → Merge and optimize Parquet files (configurable size limits)
5. **Validate** → Check data integrity and missing dates
6. **Features** → Generate advanced features (imbalance dollar bars using Dask/Numba)

### Key Components
- **main.py**: Central orchestrator with interactive CLI and command-line interface
- **src/data_pipeline/**: Core ETL modules
  - **downloaders/binance_downloader.py**: Downloads from Binance with checksum verification
  - **extractors/csv_extractor.py**: ZIP extraction and CSV validation
  - **converters/csv_to_parquet.py**: CSV to Parquet conversion with auto-cleanup
  - **processors/parquet_optimizer.py**: Merges and optimizes Parquet files
  - **processors/parquet_merger.py**: Merges daily updates into optimized files
  - **validators/missing_dates_validator.py**: Validates data completeness
- **src/features/imbalance_bars.py**: Generates imbalance dollar bars using Dask distributed computing
- **datasets/**: Data storage with progress tracking (JSON files)
- **output/**: Generated features and processed data

### Design Principles
- **Integrity First**: Every step includes validation and checksum verification
- **Resume Capability**: Progress tracking allows resuming interrupted operations
- **Memory Efficient**: Uses Dask for distributed processing of large datasets
- **Performance Optimized**: Numba JIT compilation for compute-intensive operations
- **Modular**: Each pipeline step is independent and can be run separately

### Data Types Supported
- Markets: spot, futures (USD-M), futures (COIN-M)
- Granularities: daily, monthly
- Formats: ZIP → CSV → Parquet → Optimized Parquet

### Progress Tracking
Progress is saved in JSON files (e.g., `download_progress_BTCUSDT_spot_daily.json`) allowing operations to resume from interruption points.

### Important Patterns
- Use interactive mode for exploring options
- Always validate data after optimization
- Check logs in `datasets/logs/` for detailed processing information
- Parquet files are auto-cleaned after successful conversion
- Missing dates validator can check both file-level and daily gaps
- Daily data updates are merged into existing optimized files

### Special Features
- **Add Missing Daily Data**: Automatically detects and downloads missing recent data (menu option 7)
- **ZIP Cleanup**: Removes ZIP and CHECKSUM files to free disk space (menu option 6)
- **Integrity-First Pipeline**: Step 2 validates data at every stage, stopping on corruption