# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Running the Application
```bash
# Interactive mode (recommended)
python main.py

# Command-line mode examples
python main.py download --symbol BTCUSDT --type spot --granularity daily
python main.py optimize --source data/raw --target data/optimized
python main.py validate --symbol BTCUSDT
python main.py features --type imbalance
```

### Setup
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Architecture

This is a cryptocurrency data pipeline for machine learning, focused on simplicity and performance using Parquet files.

### Data Flow
1. **Download** → Binance historical data (ZIP files)
2. **Extract** → ZIP to CSV conversion
3. **Convert** → CSV to Parquet format
4. **Optimize** → Merge and compress Parquet files
5. **Validate** → Check data integrity
6. **Features** → Generate Imbalance Dollar Bars

### Key Components
- **main.py**: Central entry point with interactive CLI
- **src/data_pipeline/**: Core ETL modules
  - downloaders/binance_downloader.py
  - extractors/csv_extractor.py
  - converters/csv_to_parquet.py
  - processors/parquet_optimizer.py
  - validators/missing_dates_validator.py
- **src/features/**: Feature engineering
  - imbalance_bars.py
  - imbalance_dollar_bars.py
  - standard_dollar_bars.py
- **data/**: All data storage (Parquet files)
- **output/**: Generated features

### Design Principles
- **Simple**: No Docker, databases, or complex infrastructure
- **Fast**: Parquet files for 10x better performance than CSV
- **Reliable**: Checksum verification and data validation
- **Resumable**: Progress tracking for interrupted operations

### Working with the Code
- All paths use "data/" not "datasets/"
- Focus on main.py as the entry point
- Use Parquet files directly - no database needed
- Dask handles files larger than RAM automatically