# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Commands

### Running the Pipeline
```bash
# Interactive mode (recommended for new users)
python main.py

# Install dependencies
pip install -r requirements.txt

# Direct command examples
python main.py download --start 2024-01-01 --end 2024-01-31
python main.py optimize --source data/raw --target data/optimized
python main.py validate --data-dir datasets/dataset-raw-daily-compressed-optimized/spot --symbol BTCUSDT
python main.py features --type imbalance

# Utility scripts
python reoptimize_parquet_files_streaming.py
python cleanup_merged_daily_files.py
```

### Development
- **No test suite currently exists** - tests would need to be created from scratch
- **No linting configuration** - consider adding ruff, black, or flake8
- **No type checking** - consider adding mypy
- Dependencies are managed via `requirements.txt`

## High-Level Architecture

This is a sophisticated Bitcoin/cryptocurrency ML finance data pipeline that processes Binance trading data through a 6-stage progressive refinement process:

### Data Flow Architecture
```
Raw Data (ZIP) → CSV → Basic Parquet → Optimized Parquet → Features → Analytics
```

### Key Pipeline Stages

**Stage 1: Data Acquisition** (`src/data_pipeline/downloaders/binance_downloader.py`)
- Downloads ZIP files from Binance's public data API
- Supports spot and futures markets (USD-M, COIN-M)
- Concurrent downloading with checksum verification
- Smart resume capability with progress tracking

**Stage 2: Integrated ZIP → CSV → Parquet Pipeline** (`run_zip_to_parquet_pipeline`)
- **Integrity-first processing**: Processes each ZIP file individually with fail-fast approach
- **Extract CSV from ZIP**: Uses CSVExtractor to extract individual CSV files
- **Immediate integrity validation**: Verifies CSV data quality and structure before conversion
- **Convert to Parquet**: Only converts validated CSV files to optimized Parquet format
- **Automatic cleanup**: Deletes CSV files after successful Parquet conversion
- **Error handling**: Stops pipeline immediately on any integrity failure
- **Backup preservation**: Keeps ZIP files as backup for investigation

**Stage 3: Parquet Validation** (`run_parquet_validation_step`)
- **Comprehensive file integrity checks**: Validates Parquet file readability and structure
- **Schema validation**: Ensures all expected columns are present and correctly typed
- **Data quality assessment**: Checks row counts, timestamp ranges, and data completeness
- **Performance metrics**: Reports total rows processed and processing statistics

**Stage 4: Data Optimization** (`src/data_pipeline/processors/parquet_optimizer.py`)
- Consolidates small Parquet files into 10GB optimized files
- Schema standardization and comprehensive integrity verification
- Safe cleanup with user confirmation

**Stage 5: Advanced Processing** (`src/data_pipeline/processors/parquet_merger.py`)
- **Intelligent daily data merging** into existing optimized files
- **Memory-efficient streaming** re-optimization
- **Automatic cleanup** of temporary files after successful merge
- **Incremental processing** without loading entire datasets

**Stage 6: Feature Engineering** (`src/features/imbalance_bars.py`)
- Generates imbalance dollar bars using advanced quantitative finance techniques
- Numba-optimized processing with Dask distributed computing
- EWMA-based adaptive thresholds for market microstructure analysis

### Data Types Supported
- **Markets**: Spot trading, Futures USD-M, Futures COIN-M
- **Granularities**: Daily and monthly data
- **Fields**: trade_id, price, qty, quoteQty, time, isBuyerMaker, isBestMatch

### Production-Ready Features
- **Comprehensive error handling** with retry mechanisms and exponential backoff
- **Robust validation** at every stage with missing date detection
- **Memory-efficient processing** using streaming and chunking
- **Concurrent processing** with configurable worker threads
- **Progress tracking** with JSON-based state management and resumable operations
- **Extensive logging** for debugging and monitoring

### Pipeline Steps Overview

The pipeline has been redesigned with integrity-first approach into 10 main steps:

1. **📥 Download ZIP data with checksum verification** - Downloads ZIP files with checksum verification only
2. **🔄 ZIP → CSV → Parquet Pipeline (Integrity-First)** - Integrated pipeline that extracts CSV from ZIP, validates integrity, converts to Parquet, and stops on any integrity failure
3. **🔍 Validate Parquet files integrity** - Comprehensive validation of Parquet files including schema, row counts, and data quality
4. **🔧 Optimize Parquet files** - Consolidates files into 10GB optimized Parquet files
5. **📅 Validate missing dates** - Checks for data gaps and integrity issues
6. **📊 Generate features** - Creates advanced features like imbalance bars
7. **🗑️ Clean ZIP and CHECKSUM files** - Removes temporary files to free disk space
8. **📅 Add missing daily data** - Automatically detects and adds missing daily data with intelligent merging
9. **🔧 Re-optimize parquet files (streaming)** - Memory-efficient re-optimization for files not reaching 10GB target
10. **🚪 Exit** - Exit the pipeline

### Directory Structure
```
src/
├── data_pipeline/          # Core ETL pipeline
│   ├── downloaders/        # Binance data downloaders
│   ├── extractors/         # CSV extraction from ZIP
│   ├── converters/         # CSV to Parquet conversion
│   ├── processors/         # Parquet optimization and merging
│   └── validators/         # Data integrity validation
├── features/               # Feature engineering (imbalance bars)
└── notebooks/              # Jupyter examples (dollar bars, imbalance bars)

datasets/                   # Data storage with organized subdirectories
├── dataset-raw-daily*/
├── dataset-raw-daily-compressed*/
└── dataset-raw-daily-compressed-optimized*/
```

### Key Technical Characteristics
- **Handles terabytes of data** efficiently with distributed processing capabilities
- **Institutional-grade quality** with comprehensive data validation
- **Modern Python ecosystem** leveraging Dask, Numba, PyArrow for performance
- **Quantitative finance focus** with advanced market microstructure features
- **User-friendly interface** with both interactive and programmatic modes

### CLI Interface
The main entry point (`main.py`) provides an interactive menu system for guided workflow and command-line interface for automation. Features include:

- **Interactive menu system** with status indicators for each pipeline step
- **Automatic daily data updates** with intelligent gap detection and merging
- **Progress tracking** with JSON-based state management for resumable operations
- **Comprehensive logging** with detailed operation logs
- **Smart configuration** with market-specific suggestions (spot/futures, daily/monthly)
- **Automatic cleanup** of temporary files after successful processing

## Working with the Codebase

When making changes:
1. **Follow the modular architecture** - each stage has clear separation of concerns
2. **Maintain data integrity** - all changes should preserve validation and error handling
3. **Use existing patterns** - leverage the established progress tracking and logging systems
4. **Test with small datasets** - the pipeline handles massive scale, so test incremental changes carefully
5. **Preserve the CLI interface** - changes should maintain backward compatibility with the interactive and command-line modes
6. **Use Numba for optimization** - follow existing patterns for performance-critical code
7. **Maintain streaming processing** - ensure memory-efficient handling of large files

## Current Development Focus

The pipeline is currently being optimized for:
- **Futures data processing** - testing the complete flow from download to optimization
- **Streaming compaction** - ensuring files reach the 10GB target size efficiently
- **Data integrity validation** - comprehensive checks for missing dates and data quality
- **Automatic daily updates** - intelligent merging of new daily data into existing optimized files

The pipeline is designed for quantitative finance research, high-frequency trading strategy development, market microstructure analysis, and machine learning feature engineering on cryptocurrency trading data.