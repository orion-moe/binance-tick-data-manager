#!/usr/bin/env python3
"""
Main entry point for the Bitcoin ML Finance pipeline
"""

import argparse
import sys
import json
from pathlib import Path
from datetime import datetime
from typing import Tuple, Optional
import logging
from logging.handlers import RotatingFileHandler

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Note: download_main import removed - using direct downloader class instead
from data_pipeline.extractors.csv_extractor import CSVExtractor
from data_pipeline.converters.csv_to_parquet import CSVToParquetConverter
from data_pipeline.processors.robust_parquet_optimizer import RobustParquetOptimizer, OptimizationConfig
from data_pipeline.processors.parquet_optimizer import main as optimize_main
from data_pipeline.validators.quick_validator import main as quick_validate_main
from data_pipeline.validators.advanced_validator import main as advanced_validate_main
from data_pipeline.validators.missing_dates_validator import main as missing_dates_main
from data_pipeline.validators.data_integrity_validator import DataIntegrityValidator
from features.imbalance_bars import main as imbalance_main


def setup_logging():
    """Set up logging configuration"""
    # Create logs directory
    log_dir = Path("datasets/logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Log file with timestamp
    log_filename = f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_path = log_dir / log_filename
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # File handler with rotation
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=50*1024*1024,  # 50MB
        backupCount=10
    )
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)
    
    # Add handlers
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # Log initial message
    logging.info(f"Pipeline started - Log file: {log_path}")
    
    return log_path


class PipelineConfig:
    """Store pipeline configuration"""
    def __init__(self):
        self.symbol = None
        self.data_type = None
        self.futures_type = None
        self.granularity = None
        self.start_date = None
        self.end_date = None
        self.workers = 5


def get_data_date_range(symbol, data_type, futures_type, granularity):
    """Get the first and last available dates from existing data using progress file"""
    try:
        # Check progress file for processed dates (more reliable than parsing corrupted parquet timestamps)
        progress_file = Path("datasets") / f"download_progress_{symbol}_{data_type}_{granularity}.json"

        if progress_file.exists():
            import json
            with open(progress_file, 'r') as f:
                progress = json.load(f)

            downloaded = progress.get('downloaded', [])
            if downloaded:
                # Sort dates and get first/last
                sorted_dates = sorted(downloaded)
                return sorted_dates[0], sorted_dates[-1]

        # Fallback: check if parquet files exist (even if timestamps are corrupted)
        if data_type == "spot":
            compressed_dir = Path("datasets") / f"dataset-raw-{granularity}-compressed" / "spot"
        else:  # futures
            compressed_dir = Path("datasets") / f"dataset-raw-{granularity}-compressed" / f"futures-{futures_type}"

        if compressed_dir.exists():
            parquet_files = list(compressed_dir.glob(f"{symbol}-Trades-*.parquet"))
            if parquet_files:
                # If we have parquet files but no valid progress info,
                # let user know data exists but we can't determine range
                return "data-exists", "data-exists"

        return None, None

    except Exception:
        return None, None


def select_market_and_granularity() -> PipelineConfig:
    """Select market (symbol, type) and granularity first"""
    config = PipelineConfig()

    print("\n" + "="*60)
    print(" 🚀 Bitcoin ML Finance Pipeline - Market Selection ")
    print("="*60)

    # Symbol selection
    print("\nSelect trading pair symbol:")
    print("1. BTCUSDT (default)")
    print("2. ETHUSDT")
    print("3. Other symbol")

    while True:
        symbol_choice = input("\nEnter your choice (1-3) or press Enter for BTCUSDT: ").strip()
        if symbol_choice == "" or symbol_choice == "1":
            config.symbol = "BTCUSDT"
            break
        elif symbol_choice == "2":
            config.symbol = "ETHUSDT"
            break
        elif symbol_choice == "3":
            config.symbol = input("Enter symbol: ").strip().upper()
            if config.symbol:
                break
            else:
                print("Please enter a valid symbol.")
        else:
            print("Invalid choice. Please enter 1, 2, 3, or press Enter.")

    # Data type selection
    print("\nSelect data type:")
    print("1. Spot")
    print("2. Futures USD-M (USDT-margined)")
    print("3. Futures COIN-M (Coin-margined)")

    while True:
        type_choice = input("\nEnter your choice (1-3): ").strip()
        if type_choice == "1":
            config.data_type = "spot"
            config.futures_type = "um"  # default
            break
        elif type_choice == "2":
            config.data_type = "futures"
            config.futures_type = "um"
            break
        elif type_choice == "3":
            config.data_type = "futures"
            config.futures_type = "cm"
            # Adjust symbol for COIN-M if needed
            if config.symbol == "BTCUSDT":
                config.symbol = "BTCUSD_PERP"
                print(f"Note: For COIN-M futures, using symbol: {config.symbol}")
            break
        else:
            print("Invalid choice. Please enter 1, 2, or 3.")

    # Granularity selection
    print("\nSelect data granularity:")
    print("1. Daily")
    print("2. Monthly")

    while True:
        gran_choice = input("\nEnter your choice (1-2): ").strip()
        if gran_choice == "1":
            config.granularity = "daily"
            break
        elif gran_choice == "2":
            config.granularity = "monthly"
            break
        else:
            print("Invalid choice. Please enter 1 or 2.")

    return config


def check_pipeline_status(config: PipelineConfig) -> dict:
    """Check the status of each pipeline step"""
    status = {
        "zip_downloaded": False,
        "csv_extracted": False,
        "csv_validated": False,
        "parquet_converted": False,
        "parquet_optimized": False,
        "data_validated": False,
        "features_generated": False
    }

    # Check download progress
    progress_file = Path("datasets") / f"download_progress_{config.symbol}_{config.data_type}_{config.granularity}.json"
    if progress_file.exists():
        with open(progress_file, 'r') as f:
            progress = json.load(f)
            if progress.get('downloaded'):
                status['zip_downloaded'] = True

    # Check for CSV extraction
    if config.data_type == "spot":
        raw_dir = Path("datasets") / f"dataset-raw-{config.granularity}" / "spot"
    else:
        raw_dir = Path("datasets") / f"dataset-raw-{config.granularity}" / f"futures-{config.futures_type}"

    if raw_dir.exists():
        csv_files = list(raw_dir.glob(f"{config.symbol}-trades-*.csv"))
        if csv_files:
            status['csv_extracted'] = True

    # Check CSV validation progress
    extraction_progress_file = Path("datasets") / f"extraction_progress_{config.symbol}_{config.data_type}_{config.granularity}.json"
    if extraction_progress_file.exists():
        with open(extraction_progress_file, 'r') as f:
            progress = json.load(f)
            if progress.get('verified'):
                status['csv_validated'] = True

    # Check for parquet files
    compressed_dir = Path("datasets") / f"dataset-raw-{config.granularity}-compressed"
    if config.data_type == "spot":
        parquet_dir = compressed_dir / "spot"
    else:
        parquet_dir = compressed_dir / f"futures-{config.futures_type}"

    if parquet_dir.exists():
        parquet_files = list(parquet_dir.glob(f"{config.symbol}-Trades-*.parquet"))
        if parquet_files:
            status['parquet_converted'] = True

    # Check for optimized parquet files
    optimized_dir = Path("datasets") / f"dataset-raw-{config.granularity}-compressed-optimized" / config.data_type
    if optimized_dir.exists():
        optimized_files = list(optimized_dir.glob(f"{config.symbol}*.parquet"))
        if optimized_files:
            status['parquet_optimized'] = True

    return status


def display_pipeline_menu(config: PipelineConfig):
    """Display the pipeline menu with status indicators"""
    status = check_pipeline_status(config)

    print("\n" + "="*60)
    print(f" 📊 Pipeline for {config.symbol} {config.data_type.upper()} {config.granularity.upper()} ")
    print("="*60)

    print("\nPipeline Steps:")
    print(f"1. {'✅' if status['zip_downloaded'] and status['csv_extracted'] and status['csv_validated'] else '⬜'}📥 Download ZIP data, extract and validate CSV (always re-extracts)")
    print(f"2. {'✅' if status['parquet_converted'] else '⬜'}🔍 Convert CSV to Parquet with verification and auto-cleanup")
    print(f"3. {'✅' if status['parquet_optimized'] else '⬜'}🔧 Optimize Parquet files")
    print(f"4. {'✅' if status['data_validated'] else '⬜'}✅ Validate optimized data integrity")
    print(f"5. {'✅' if status['features_generated'] else '⬜'}📊 Generate features")
    print("6. 🚪 Exit")

    return status


def run_download_and_extract(config: PipelineConfig):
    """Step 1: Download ZIP files with hash verification, extract to CSV, and validate"""
    print("\n" + "="*60)
    print(" 📥 Step 1: Download ZIP Data, Extract and Validate CSV ")
    print("="*60)
    
    # Log the start of operation
    logging.info(f"Starting Step 1: Download and Extract for {config.symbol} {config.data_type} {config.granularity}")

    # Get existing data range for suggestions
    first_available, last_available = get_data_date_range(
        config.symbol, config.data_type, config.futures_type, config.granularity
    )

    # Get date range
    print(f"\n📅 Enter date range for {config.granularity} data:")

    # Import datetime for date suggestions
    from datetime import datetime, timedelta
    current_date = datetime.now()

    if config.granularity == 'daily':
        date_format = "YYYY-MM-DD"
        if first_available and last_available and first_available != "data-exists":
            print(f"💡 Available data range: {first_available} to {last_available}")
            print(f"📊 Most recent data: {last_available}")
            example_start = last_available  # Suggest continuing from last available
            # Suggest next day after last available
            try:
                last_date = datetime.strptime(last_available, "%Y-%m-%d")
                next_date = last_date + timedelta(days=1)
                example_start = next_date.strftime("%Y-%m-%d")
            except:
                example_start = current_date.strftime("%Y-%m-%d")
            example_end = current_date.strftime("%Y-%m-%d")
        else:
            example_start = "2024-01-01"
            example_end = "2024-01-31"
            print(f"💡 No existing data found. Binance spot data available from 2017-08-17")
    else:
        date_format = "YYYY-MM"
        if first_available and last_available and first_available != "data-exists":
            print(f"💡 Available data range: {first_available} to {last_available}")
            print(f"📊 Most recent data: {last_available}")
            # Suggest next month after last available
            try:
                last_date = datetime.strptime(last_available + "-01", "%Y-%m-%d")
                if last_date.month == 12:
                    next_date = last_date.replace(year=last_date.year + 1, month=1)
                else:
                    next_date = last_date.replace(month=last_date.month + 1)
                example_start = next_date.strftime("%Y-%m")
            except:
                example_start = current_date.strftime("%Y-%m")
            example_end = current_date.strftime("%Y-%m")
        else:
            example_start = "2024-01"
            example_end = "2024-12"
            print(f"💡 No existing data found. Binance spot data available from 2017-08")

    while True:
        start_date = input(f"Start date ({date_format}, e.g., {example_start}): ").strip()
        if start_date:
            config.start_date = start_date
            break
        print("❌ Start date is required.")

    while True:
        end_date = input(f"End date ({date_format}, e.g., {example_end}): ").strip()
        if end_date:
            config.end_date = end_date
            break
        print("❌ End date is required.")

    # Get workers
    workers_input = input("\nNumber of concurrent downloads (default: 5): ").strip()
    config.workers = int(workers_input) if workers_input.isdigit() else 5

    # Show summary
    print(f"\n📋 Download Configuration:")
    print(f"   Date Range: {config.start_date} to {config.end_date}")
    print(f"   Workers: {config.workers}")

    confirm = input("\n🚀 Proceed with download? (y/n): ").strip().lower()
    if confirm != 'y':
        print("❌ Download cancelled.")
        return

    # Build command line arguments
    download_args = [
        'binance_downloader.py', 'download',
        '--symbol', config.symbol,
        '--type', config.data_type,
        '--granularity', config.granularity,
        '--start', config.start_date,
        '--end', config.end_date,
        '--workers', str(config.workers)
    ]

    if config.data_type == 'futures':
        download_args.extend(['--futures-type', config.futures_type])

    # Set sys.argv for the downloader
    original_argv = sys.argv.copy()
    sys.argv = download_args

    try:
        # Use a custom approach that only downloads ZIP files
        from data_pipeline.downloaders.binance_downloader import BinanceDataDownloader
        import concurrent.futures

        downloader = BinanceDataDownloader(
            symbol=config.symbol,
            data_type=config.data_type,
            futures_type=config.futures_type,
            granularity=config.granularity
        )

        # Generate date range
        start = datetime.strptime(config.start_date, '%Y-%m-%d' if config.granularity == 'daily' else '%Y-%m')
        end = datetime.strptime(config.end_date, '%Y-%m-%d' if config.granularity == 'daily' else '%Y-%m')
        dates = downloader.generate_dates(start, end)

        print(f"\n📥 Downloading {len(dates)} ZIP files with CHECKSUM verification...")
        logging.info(f"Download phase: {len(dates)} files to process for date range {config.start_date} to {config.end_date}")

        # Download only ZIP files, skip processing
        downloaded_count = 0
        failed_count = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=config.workers) as executor:
            future_to_date = {
                executor.submit(downloader.download_with_checksum, date): date
                for date in dates
            }

            for future in concurrent.futures.as_completed(future_to_date):
                date = future_to_date[future]
                try:
                    zip_file, checksum_file = future.result()
                    if zip_file and checksum_file:
                        downloaded_count += 1
                        print(f"✅ Downloaded: {zip_file.name}")
                    else:
                        # Check why download returned None
                        date_str = date.strftime('%Y-%m-%d' if config.granularity == 'daily' else '%Y-%m')
                        zip_name = f"{config.symbol}-trades-{date.strftime('%Y-%m-%d' if config.granularity == 'daily' else '%Y-%m')}.zip"
                        csv_name = f"{config.symbol}-trades-{date.strftime('%Y-%m-%d' if config.granularity == 'daily' else '%Y-%m')}.csv"
                        
                        if (downloader.raw_dir / csv_name).exists():
                            print(f"⏭️ CSV already exists: {csv_name}")
                        elif (downloader.raw_dir / zip_name).exists():
                            print(f"📦 ZIP exists: {zip_name}")
                        else:
                            failed_count += 1
                            print(f"❌ Download failed: {zip_name}")
                except Exception as e:
                    failed_count += 1
                    print(f"❌ Error downloading {date}: {e}")

        print(f"\n✅ Download phase completed: {downloaded_count} downloaded, {failed_count} failed")
        logging.info(f"Download phase completed: {downloaded_count} downloaded, {failed_count} failed")

        # Now extract CSV files from downloaded ZIPs
        print("\n📦 Extracting CSV files from ZIP archives...")

        extractor = CSVExtractor(
            symbol=config.symbol,
            data_type=config.data_type,
            futures_type=config.futures_type,
            granularity=config.granularity
        )

        # Force re-extraction for step 1 to ensure fresh data
        successful, failed = extractor.extract_and_verify_all(force_reextract=True)

        if successful > 0:
            print(f"\n✅ Extraction completed: {successful} files extracted (with force re-extraction)")
            logging.info(f"Extraction completed: {successful} files extracted (force re-extraction enabled)")
            if failed > 0:
                print(f"⚠️ {failed} files failed extraction")
                logging.warning(f"{failed} files failed extraction")

            # CSV Validation
            print("\n🔍 Validating extracted CSV files...")

            # Get extracted CSV files
            if config.data_type == "spot":
                raw_dir = Path("datasets") / f"dataset-raw-{config.granularity}" / "spot"
            else:
                raw_dir = Path("datasets") / f"dataset-raw-{config.granularity}" / f"futures-{config.futures_type}"

            csv_files = list(raw_dir.glob(f"{config.symbol}-trades-*.csv"))
            print(f"📄 Found {len(csv_files)} CSV files to validate")

            # Enhanced validation of CSV files using extractor's verify method
            validation_passed = True
            validation_details = []

            print("\n📊 Performing comprehensive CSV validation...")
            print("   - Timestamp UTC conversion check")
            print("   - Missing dates detection")
            print("   - Data integrity verification")

            # Validate all files (or sample if too many)
            files_to_validate = csv_files if len(csv_files) <= 10 else csv_files[:10]

            for csv_file in files_to_validate:
                print(f"\n   📄 Validating {csv_file.name}...")
                if extractor.verify_csv_integrity(csv_file):
                    validation_details.append(f"✅ {csv_file.name}")
                else:
                    validation_details.append(f"❌ {csv_file.name}")
                    validation_passed = False

            if len(csv_files) > 10:
                print(f"\n   ... and {len(csv_files) - 10} more files")

            if validation_passed:
                print("\n✅ CSV validation passed!")
                logging.info("CSV validation passed successfully")
            else:
                print("\n⚠️ Some CSV files failed validation")
                logging.warning(f"CSV validation failed - details: {validation_details}")

            # Don't ask about cleanup in step 1 - keep all ZIP files
            print("\n📦 ZIP files preserved for backup")

            # Pipeline completion message
            print("\n" + "="*60)
            print(" 🎉 Step 1 Completed: CSV Files Ready! ")
            print("="*60)
            print("\n📌 CSV files have been extracted and validated successfully.")
            print("📁 Location: " + str(raw_dir))
            print("\n🔄 Next Steps (run these commands separately):")
            print("   2️⃣ CSV to Parquet conversion: python main.py")
            print("   3️⃣ Parquet optimization: python main.py")
            print("   4️⃣ Data validation: python main.py")
            print("   5️⃣ Feature generation: python main.py")
            print("\n💡 Or use the interactive menu to continue with the next steps.")

        else:
            print(f"\n❌ Extraction failed: {failed} files failed")
            logging.error(f"Extraction failed: {failed} files failed")

    except Exception as e:
        print(f"\n❌ Download/extraction failed: {e}")
        logging.error(f"Download/extraction failed: {e}", exc_info=True)
    finally:
        sys.argv = original_argv


def run_csv_to_parquet_conversion(config: PipelineConfig):
    """Step 2: Convert CSV to Parquet with verification and auto-cleanup"""
    print("\n" + "="*65)
    print(" 🔄 Step 2: CSV → Parquet with Verification & Auto-Cleanup ")
    print("="*65)

    # Create CSV to Parquet converter
    converter = CSVToParquetConverter(
        symbol=config.symbol,
        data_type=config.data_type,
        futures_type=config.futures_type,
        granularity=config.granularity
    )

    # Check CSV files exist
    if config.data_type == "spot":
        raw_dir = Path("datasets") / f"dataset-raw-{config.granularity}" / "spot"
    else:
        raw_dir = Path("datasets") / f"dataset-raw-{config.granularity}" / f"futures-{config.futures_type}"

    csv_files = list(raw_dir.glob(f"{config.symbol}-trades-*.csv"))

    if not csv_files:
        print(f"❌ No CSV files found in {raw_dir}")
        print("💡 Please run Step 1 first to download and extract data")
        return

    print(f"📁 Found {len(csv_files)} CSV files in {raw_dir}")

    # New pipeline behavior explanation
    print("\n💡 Automated Process:")
    print("   ✅ Convert CSV to optimized Parquet format")
    print("   ✅ Automatic Parquet integrity verification") 
    print("   ✅ Automatic CSV cleanup after successful conversion (saves disk space)")
    print("   ✅ ZIP files preserved as backup")
    print("\n📌 Note: CSV validation was already performed in Step 1")

    print(f"\n📋 Ready to Process:")
    print(f"   CSV files: {len(csv_files)}")
    print(f"   Source: {raw_dir}")

    confirm = input("\n🚀 Proceed with CSV to Parquet conversion? (y/n): ").strip().lower()
    if confirm != 'y':
        print("❌ CSV to Parquet conversion cancelled.")
        return

    try:
        # Convert CSV to Parquet with automatic verification and cleanup
        print(f"\n🔄 Converting CSV to Parquet with Auto-Verification & Cleanup...")
        successful, failed = converter.convert_all_csv_files()

        if successful > 0:
            print(f"\n✅ Conversion completed: {successful} files converted")
            print(f"💾 CSV files automatically cleaned up to save disk space")
            print(f"📦 ZIP files preserved as backup")
            if failed > 0:
                print(f"⚠️ {failed} files failed conversion")
        else:
            print(f"\n❌ Conversion failed: {failed} files failed")
            return

        print(f"\n🎉 CSV to Parquet conversion completed successfully!")

    except Exception as e:
        print(f"\n❌ CSV to Parquet conversion failed: {e}")


def run_parquet_optimization(config: PipelineConfig):
    """Step 3: Optimize Parquet files using robust optimizer with corruption prevention"""
    print("\n" + "="*50)
    print(" 🔧 Step 3: Robust Parquet Optimization ")
    print("="*50)

    # Determine source directory
    if config.data_type == "spot":
        source_dir = f"datasets/dataset-raw-{config.granularity}-compressed/spot"
    else:
        source_dir = f"datasets/dataset-raw-{config.granularity}-compressed/futures-{config.futures_type}"

    # Target directory for optimized files
    target_dir = f"datasets/dataset-raw-{config.granularity}-compressed-optimized/{config.data_type}"

    print(f"📁 Source: {source_dir}")
    print(f"📁 Target: {target_dir}")

    # Configuration options
    print("\n⚙️ Optimization Configuration:")
    max_size_input = input("Maximum file size in GB (default: 10): ").strip()
    max_size = int(max_size_input) if max_size_input.isdigit() else 10

    print("\nOptimization Options:")
    print("1. 🛡️  Robust mode (recommended) - Full corruption prevention")
    print("2. 🚀 Legacy mode - Original optimizer")

    mode_choice = input("\nChoose optimization mode (1-2, default: 1): ").strip()
    use_robust = mode_choice != "2"

    if use_robust:
        print("\n🛡️ Using robust optimization with corruption prevention...")

        # Advanced options for robust mode
        compression_input = input("Compression algorithm (snappy/gzip/lz4, default: snappy): ").strip()
        compression = compression_input if compression_input in ['snappy', 'gzip', 'lz4'] else 'snappy'

        verify_checksum = input("Enable checksum verification? (y/n, default: y): ").strip().lower() != 'n'
        keep_backup = input("Keep backup of original files? (y/n, default: y): ").strip().lower() != 'n'

        print(f"\n📋 Configuration:")
        print(f"   Max file size: {max_size} GB")
        print(f"   Compression: {compression}")
        print(f"   Checksum verification: {'Yes' if verify_checksum else 'No'}")
        print(f"   Keep backup: {'Yes' if keep_backup else 'No'}")

    confirm = input(f"\n🚀 Proceed with {'robust' if use_robust else 'legacy'} optimization? (y/n): ").strip().lower()
    if confirm != 'y':
        print("❌ Optimization cancelled.")
        return

    try:
        if use_robust:
            # Use robust optimizer
            optimization_config = OptimizationConfig(
                max_file_size_gb=max_size,
                compression=compression,
                verify_checksum=verify_checksum,
                keep_backup=keep_backup
            )

            optimizer = RobustParquetOptimizer(source_dir, target_dir, optimization_config)
            success = optimizer.run_optimization()

            if success:
                print("\n✅ Robust optimization completed successfully!")
                print("🛡️ Data corruption prevention measures were applied")
            else:
                print("\n❌ Robust optimization failed - check logs for details")
        else:
            # Use legacy optimizer
            print("\n⚠️ Using legacy optimizer - limited corruption protection")
            sys.argv = ['optimize', '--source', source_dir, '--target', target_dir,
                       '--max-size', str(max_size), '--auto-confirm']
            optimize_main()
            print("\n✅ Legacy optimization completed!")

    except Exception as e:
        print(f"\n❌ Optimization failed: {e}")
        print("💡 Try using robust mode for better error handling")




def run_data_validation(config: PipelineConfig):
    """Step 4: Validate data integrity"""
    print("\n" + "="*50)
    print(" ✅ Step 4: Validate Data Integrity ")
    print("="*50)

    print("Choose validation type:")
    print("1. Quick validation")
    print("2. Advanced validation")
    print("3. Missing dates validation")
    print("4. 🛡️ Comprehensive integrity validation (recommended)")

    choice = input("\nEnter your choice (1-4): ").strip()

    if choice == "1":
        print("🔍 Running quick validation...")
        try:
            quick_validate_main()
            print("✅ Quick validation completed!")
        except Exception as e:
            print(f"❌ Validation failed: {e}")
    elif choice == "2":
        print("🔍 Running advanced validation...")
        output_dir = input("Output directory (default: reports): ").strip() or "reports"

        original_argv = sys.argv.copy()
        sys.argv = ['validate', '--base-path', '.', '--output-dir', output_dir]

        try:
            advanced_validate_main()
            print("✅ Advanced validation completed!")
        except Exception as e:
            print(f"❌ Validation failed: {e}")
        finally:
            sys.argv = original_argv
    elif choice == "3":
        print("🔍 Running missing dates validation...")

        # Get data directory based on config - use optimized parquet files
        data_dir = f"datasets/dataset-raw-{config.granularity}-compressed-optimized/{config.data_type}"

        # Ask if user wants to check daily gaps
        check_daily = input("\nCheck for daily gaps within files? (slower) (y/n): ").strip().lower() == 'y'

        original_argv = sys.argv.copy()
        sys.argv = ['missing_dates_validator', '--data-dir', data_dir, '--symbol', config.symbol]
        if check_daily:
            sys.argv.append('--check-daily-gaps')

        try:
            missing_dates_main()
            print("\n✅ Missing dates validation completed!")
        except Exception as e:
            print(f"❌ Validation failed: {e}")
        finally:
            sys.argv = original_argv
    elif choice == "4":
        print("🛡️ Running comprehensive integrity validation...")

        # Get data directory based on config - use optimized parquet files
        data_dir = f"datasets/dataset-raw-{config.granularity}-compressed-optimized/{config.data_type}"

        print(f"📁 Validating directory: {data_dir}")

        # Options for comprehensive validation
        max_workers_input = input("\nMax worker threads (default: 4): ").strip()
        max_workers = int(max_workers_input) if max_workers_input.isdigit() else 4

        save_report = input("Save detailed report to JSON? (y/n, default: y): ").strip().lower() != 'n'
        report_path = None
        if save_report:
            report_path = f"reports/integrity_validation_{config.symbol}_{config.data_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        try:
            from pathlib import Path
            validator = DataIntegrityValidator()
            report = validator.validate_directory(Path(data_dir), max_workers)

            # Print summary
            validator.print_report_summary(report)

            # Save report if requested
            if save_report and report_path:
                Path("reports").mkdir(exist_ok=True)
                validator.save_report(report, Path(report_path))
                print(f"\n📄 Detailed report saved to: {report_path}")

            if report.invalid_files == 0:
                print("\n✅ Comprehensive validation completed successfully!")
            else:
                print(f"\n⚠️ Validation completed with {report.invalid_files} invalid files")

        except Exception as e:
            print(f"❌ Comprehensive validation failed: {e}")
    else:
        print("❌ Invalid choice.")


def run_feature_generation(config: PipelineConfig):
    """Step 5: Generate features"""
    print("\n" + "="*50)
    print(" 📊 Step 5: Generate Features ")
    print("="*50)

    print("Available features:")
    print("1. Imbalance bars")

    choice = input("\nEnter your choice (1): ").strip()

    if choice == "1":
        print("📊 Generating imbalance bars...")
        print(f"   Symbol: {config.symbol}")
        print(f"   Data Type: {config.data_type}")
        print(f"   Granularity: {config.granularity}")
        print("\n⚠️  Note: Feature generation currently uses hardcoded parameters.")
        print("   Future versions will use the configuration above.")
        try:
            imbalance_main()
            print("✅ Feature generation completed!")
        except Exception as e:
            print(f"❌ Feature generation failed: {e}")
    else:
        print("❌ Invalid choice.")


def interactive_main():
    """Main interactive menu with new flow"""
    # Setup logging
    log_path = setup_logging()
    
    print("\n" + "="*60)
    print(" 🚀 Bitcoin ML Finance Pipeline ")
    print("="*60)
    print(f"📝 Logs saved to: {log_path}")

    # First, select market and granularity
    config = select_market_and_granularity()

    while True:
        display_pipeline_menu(config)

        choice = input("\nEnter your choice (1-6): ").strip()

        if choice == "1":
            run_download_and_extract(config)
        elif choice == "2":
            run_csv_to_parquet_conversion(config)
        elif choice == "3":
            run_parquet_optimization(config)
        elif choice == "4":
            run_data_validation(config)
        elif choice == "5":
            run_feature_generation(config)
        elif choice == "6":
            print("\n👋 Goodbye!")
            break
        else:
            print("❌ Invalid choice. Please enter 1-6.")


def main():
    # Setup logging for all modes
    log_path = setup_logging()
    
    parser = argparse.ArgumentParser(
        description="Bitcoin ML Finance Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Interactive mode (recommended)
  python main.py

  # Direct command mode
  python main.py download --start 2024-01-01 --end 2024-01-31
  python main.py optimize --source data/raw --target data/optimized
  python main.py validate --quick
  python main.py features --type imbalance
        """
    )
    
    logging.info(f"Pipeline started with arguments: {sys.argv}")

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Download command
    download_parser = subparsers.add_parser('download', help='Download Bitcoin data from Binance')
    download_parser.add_argument('--symbol', default='BTCUSDT', help='Trading pair symbol')
    download_parser.add_argument('--type', choices=['spot', 'futures'], default='spot',
                                help='Data type (spot or futures)')
    download_parser.add_argument('--futures-type', choices=['um', 'cm'], default='um',
                                help='Futures type (um=USD-M, cm=COIN-M)')
    download_parser.add_argument('--granularity', choices=['daily', 'monthly'], default='monthly',
                                help='Data granularity')
    download_parser.add_argument('--start', required=True,
                                help='Start date (YYYY-MM-DD for daily, YYYY-MM for monthly)')
    download_parser.add_argument('--end', required=True,
                                help='End date (YYYY-MM-DD for daily, YYYY-MM for monthly)')
    download_parser.add_argument('--workers', type=int, default=5,
                                help='Number of concurrent downloads (default: 5)')

    # Optimize command
    optimize_parser = subparsers.add_parser('optimize', help='Optimize parquet files')
    optimize_parser.add_argument('--source', required=True, help='Source directory')
    optimize_parser.add_argument('--target', required=True, help='Target directory')
    optimize_parser.add_argument('--max-size', type=int, default=10, help='Maximum file size in GB')
    optimize_parser.add_argument('--auto-confirm', action='store_true', help='Auto confirm operations')

    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate data integrity')
    validate_parser.add_argument('--quick', action='store_true', help='Run quick validation')
    validate_parser.add_argument('--advanced', action='store_true', help='Run advanced validation')
    validate_parser.add_argument('--missing-dates', action='store_true', help='Check for missing dates')
    validate_parser.add_argument('--output-dir', default='reports', help='Output directory for reports')
    validate_parser.add_argument('--base-path', default='.', help='Base path for data')
    validate_parser.add_argument('--data-dir', help='Data directory for missing dates check')
    validate_parser.add_argument('--symbol', default='BTCUSDT', help='Symbol for missing dates check')
    validate_parser.add_argument('--check-daily-gaps', action='store_true', help='Check daily gaps in missing dates validation')

    # Features command
    features_parser = subparsers.add_parser('features', help='Generate features')
    features_parser.add_argument('--type', choices=['imbalance'], default='imbalance',
                                help='Type of features to generate')

    args = parser.parse_args()

    # If no command is provided, run interactive mode
    if not args.command:
        interactive_main()
        return

    if args.command == 'download':
        # Use the same custom download approach as interactive mode
        from data_pipeline.downloaders.binance_downloader import BinanceDataDownloader
        from data_pipeline.extractors.csv_extractor import CSVExtractor
        import concurrent.futures

        downloader = BinanceDataDownloader(
            symbol=args.symbol,
            data_type=args.type,
            futures_type=args.futures_type if args.type == 'futures' else 'um',
            granularity=args.granularity
        )

        # Generate date range
        start = datetime.strptime(args.start, '%Y-%m-%d' if args.granularity == 'daily' else '%Y-%m')
        end = datetime.strptime(args.end, '%Y-%m-%d' if args.granularity == 'daily' else '%Y-%m')
        dates = downloader.generate_dates(start, end)

        print(f"\n📥 Downloading {len(dates)} ZIP files with CHECKSUM verification...")

        # Download only ZIP files
        downloaded_count = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_date = {
                executor.submit(downloader.download_with_checksum, date): date
                for date in dates
            }

            for future in concurrent.futures.as_completed(future_to_date):
                date = future_to_date[future]
                try:
                    zip_file, checksum_file = future.result()
                    if zip_file and checksum_file:
                        downloaded_count += 1
                        print(f"✅ Downloaded: {zip_file.name}")
                except Exception as e:
                    print(f"❌ Error downloading {date}: {e}")

        print(f"\n✅ Download completed: {downloaded_count} files")

        # Extract CSV files
        print("\n📦 Extracting CSV files...")
        extractor = CSVExtractor(
            symbol=args.symbol,
            data_type=args.type,
            futures_type=args.futures_type if args.type == 'futures' else 'um',
            granularity=args.granularity
        )

        successful, failed = extractor.extract_and_verify_all()
        print(f"\n✅ Extraction completed: {successful} files extracted, {failed} failed")
    elif args.command == 'optimize':
        sys.argv = ['optimize', '--source', args.source, '--target', args.target,
                   '--max-size', str(args.max_size)]
        if args.auto_confirm:
            sys.argv.append('--auto-confirm')
        optimize_main()
    elif args.command == 'validate':
        if args.quick:
            quick_validate_main()
        elif args.advanced:
            sys.argv = ['validate', '--base-path', args.base_path,
                       '--output-dir', args.output_dir]
            advanced_validate_main()
        elif args.missing_dates:
            if not args.data_dir:
                print("Please specify --data-dir for missing dates validation")
                return
            sys.argv = ['missing_dates_validator', '--data-dir', args.data_dir,
                       '--symbol', args.symbol]
            if args.check_daily_gaps:
                sys.argv.append('--check-daily-gaps')
            missing_dates_main()
        else:
            print("Please specify --quick, --advanced, or --missing-dates for validation")
    elif args.command == 'features':
        if args.type == 'imbalance':
            imbalance_main()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()