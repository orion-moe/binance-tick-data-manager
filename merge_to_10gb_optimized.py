#!/usr/bin/env python3
"""
Merge script that properly respects the 10GB file size limit
Works for both spot and futures data
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
from loguru import logger
import sys
import gc
import shutil

# Configurar logging
logger.remove()
logger.add(sys.stdout, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>")

# Constants
MAX_FILE_SIZE_GB = 10.0
SIZE_BUFFER = 0.9  # Use 90% of max size to leave buffer
TARGET_SIZE_BYTES = int(MAX_FILE_SIZE_GB * SIZE_BUFFER * 1024 * 1024 * 1024)


def get_file_info(file_path: Path) -> dict:
    """Get basic info about a parquet file"""
    try:
        pf = pq.ParquetFile(file_path)
        meta = pf.metadata
        schema = pf.schema
        
        # Get size
        size_bytes = file_path.stat().st_size
        size_gb = size_bytes / (1024**3)
        
        # Get row count
        num_rows = meta.num_rows
        
        # Get date range
        first_group = pf.read_row_group(0, columns=['time'])
        last_group = pf.read_row_group(pf.num_row_groups - 1, columns=['time'])
        
        df_first = first_group.to_pandas()
        df_last = last_group.to_pandas()
        
        first_timestamp = df_first['time'].min()
        last_timestamp = df_last['time'].max()
        
        return {
            'path': file_path,
            'size_bytes': size_bytes,
            'size_gb': size_gb,
            'num_rows': num_rows,
            'first_timestamp': first_timestamp,
            'last_timestamp': last_timestamp,
            'first_date': pd.to_datetime(first_timestamp).date(),
            'last_date': pd.to_datetime(last_timestamp).date()
        }
    except Exception as e:
        logger.error(f"Error getting info for {file_path}: {e}")
        return None


def estimate_file_size(df: pd.DataFrame) -> int:
    """Estimate the size of a dataframe when written to parquet"""
    # Create a temporary in-memory buffer
    import io
    buffer = io.BytesIO()
    
    # Write a sample (first 1000 rows) to estimate compression ratio
    sample_size = min(1000, len(df))
    sample_df = df.head(sample_size)
    
    table = pa.Table.from_pandas(sample_df)
    pq.write_table(table, buffer, compression='snappy')
    
    # Estimate full size based on sample
    sample_bytes = buffer.tell()
    estimated_bytes = (sample_bytes / sample_size) * len(df)
    
    return int(estimated_bytes * 1.1)  # Add 10% safety margin


def merge_files_to_target_size(daily_files: list, output_dir: Path, symbol: str, 
                              start_number: int = 1, last_timestamp: int = None) -> int:
    """
    Merge daily files into optimized files respecting the 10GB size limit
    
    Returns: number of optimized files created
    """
    if not daily_files:
        logger.info("No files to merge")
        return 0
    
    # Sort files chronologically
    daily_files = sorted(daily_files)
    
    current_dfs = []
    current_size = 0
    current_rows = 0
    file_number = start_number
    files_created = 0
    
    for idx, file_path in enumerate(daily_files):
        try:
            # Read the daily file
            df = pd.read_parquet(file_path)
            
            # Filter by timestamp if provided (for continuing from existing optimized files)
            if last_timestamp is not None:
                df = df[df['time'] > last_timestamp]
                if df.empty:
                    continue
            
            # Estimate size if we add this file
            estimated_size = estimate_file_size(df)
            
            # Check if adding this file would exceed the limit
            if current_dfs and (current_size + estimated_size > TARGET_SIZE_BYTES):
                # Write current batch to file
                combined_df = pd.concat(current_dfs, ignore_index=True)
                combined_df = combined_df.sort_values('time').reset_index(drop=True)
                
                output_path = output_dir / f"{symbol}-Trades-Optimized-{file_number:03d}.parquet"
                
                logger.info(f"Writing {output_path.name} with {current_rows:,} rows (~{current_size / (1024**3):.2f} GB)")
                
                table = pa.Table.from_pandas(combined_df)
                pq.write_table(table, output_path, compression='snappy')
                
                actual_size = output_path.stat().st_size
                logger.success(f"✅ Created {output_path.name}: {actual_size / (1024**3):.2f} GB")
                
                # Reset for next file
                current_dfs = []
                current_size = 0
                current_rows = 0
                file_number += 1
                files_created += 1
                last_timestamp = None  # Reset filter after first file
                
                # Garbage collection
                del combined_df
                gc.collect()
            
            # Add current file to batch
            current_dfs.append(df)
            current_size += estimated_size
            current_rows += len(df)
            
            logger.debug(f"Added {file_path.name}: {len(df):,} rows, "
                        f"batch size: ~{current_size / (1024**3):.2f} GB")
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
            continue
    
    # Write any remaining data
    if current_dfs:
        combined_df = pd.concat(current_dfs, ignore_index=True)
        combined_df = combined_df.sort_values('time').reset_index(drop=True)
        
        output_path = output_dir / f"{symbol}-Trades-Optimized-{file_number:03d}.parquet"
        
        logger.info(f"Writing final file {output_path.name} with {current_rows:,} rows")
        
        table = pa.Table.from_pandas(combined_df)
        pq.write_table(table, output_path, compression='snappy')
        
        actual_size = output_path.stat().st_size
        logger.success(f"✅ Created {output_path.name}: {actual_size / (1024**3):.2f} GB")
        files_created += 1
    
    return files_created


def fix_undersized_optimized_files(data_type: str = "futures", futures_type: str = "um"):
    """Fix optimized files that are smaller than 10GB by re-merging them"""
    
    symbol = "BTCUSDT"
    base_dir = Path("datasets")
    
    # Set directories based on data type
    if data_type == "spot":
        optimized_dir = base_dir / "dataset-raw-daily-compressed-optimized" / "spot"
        daily_dir = base_dir / "dataset-raw-daily-compressed" / "spot"
    else:
        optimized_dir = base_dir / f"dataset-raw-daily-compressed-optimized" / f"futures-{futures_type}"
        daily_dir = base_dir / f"dataset-raw-daily-compressed" / f"futures-{futures_type}"
    
    logger.info(f"=== FIXING UNDERSIZED OPTIMIZED FILES FOR {symbol} {data_type} ===")
    
    # Find all optimized files
    optimized_files = sorted(optimized_dir.glob(f"{symbol}-Trades-Optimized-*.parquet"))
    
    if not optimized_files:
        logger.error(f"No optimized files found in {optimized_dir}")
        return
    
    # Analyze current files
    logger.info(f"\nAnalyzing {len(optimized_files)} optimized files...")
    
    undersized_files = []
    total_size = 0
    
    for file in optimized_files:
        info = get_file_info(file)
        if info:
            total_size += info['size_bytes']
            
            # Check if file is undersized (less than 90% of target)
            if info['size_gb'] < MAX_FILE_SIZE_GB * 0.9:
                undersized_files.append(info)
                logger.warning(f"{file.name}: {info['size_gb']:.2f} GB - UNDERSIZED")
            else:
                logger.info(f"{file.name}: {info['size_gb']:.2f} GB - OK")
    
    if not undersized_files:
        logger.success("✅ All files are properly sized!")
        return
    
    logger.info(f"\nFound {len(undersized_files)} undersized files")
    logger.info(f"Total size: {total_size / (1024**3):.2f} GB")
    
    # Ask for confirmation
    response = input("\nDo you want to re-merge these files to reach ~10GB each? (yes/no): ")
    if response.lower() != 'yes':
        logger.info("Operation cancelled")
        return
    
    # Create backup directory
    backup_dir = optimized_dir.parent / f"{optimized_dir.name}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    backup_dir.mkdir(exist_ok=True)
    
    # Backup all optimized files
    logger.info(f"\nBacking up files to {backup_dir}...")
    for file in optimized_files:
        shutil.copy2(file, backup_dir)
    logger.success(f"✅ Backed up {len(optimized_files)} files")
    
    # Get the first undersized file to determine where to start re-merging
    first_undersized = undersized_files[0]
    first_undersized_date = first_undersized['first_date']
    
    # Find all daily files from that date onward
    daily_files = []
    pattern = f"{symbol}-Trades-*.parquet"
    
    for file in sorted(daily_dir.glob(pattern)):
        try:
            date_str = file.stem.split('-Trades-')[-1]
            file_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            
            if file_date >= first_undersized_date:
                daily_files.append(file)
        except:
            continue
    
    logger.info(f"\nFound {len(daily_files)} daily files to re-merge")
    logger.info(f"Period: {daily_files[0].name} to {daily_files[-1].name}")
    
    # Delete undersized files
    logger.info("\nDeleting undersized files...")
    for file_info in undersized_files:
        file_info['path'].unlink()
    
    # Get the number to start from
    # Find the last properly sized file
    last_good_number = 0
    for file in optimized_files:
        info = get_file_info(file)
        if info and info['size_gb'] >= MAX_FILE_SIZE_GB * 0.9:
            number = int(file.stem.split('-')[-1])
            last_good_number = max(last_good_number, number)
    
    start_number = last_good_number + 1
    
    # Re-merge the files with proper size management
    logger.info(f"\nStarting re-merge from number {start_number:03d}...")
    
    files_created = merge_files_to_target_size(
        daily_files, optimized_dir, symbol, start_number
    )
    
    logger.success(f"\n✅ Re-merge complete! Created {files_created} new optimized files")
    
    # Show final status
    logger.info("\n=== FINAL STATUS ===")
    new_optimized_files = sorted(optimized_dir.glob(f"{symbol}-Trades-Optimized-*.parquet"))
    
    for file in new_optimized_files:
        size_gb = file.stat().st_size / (1024**3)
        logger.info(f"{file.name}: {size_gb:.2f} GB")


def main():
    """Main function with menu"""
    logger.info("=== MERGE TO 10GB OPTIMIZED FILES ===")
    logger.info("This script ensures optimized files reach ~10GB size")
    
    print("\nSelect data type to fix:")
    print("1. Futures/UM")
    print("2. Spot")
    print("3. Exit")
    
    choice = input("\nEnter choice (1-3): ")
    
    if choice == '1':
        fix_undersized_optimized_files("futures", "um")
    elif choice == '2':
        fix_undersized_optimized_files("spot")
    elif choice == '3':
        logger.info("Exiting...")
    else:
        logger.error("Invalid choice")


if __name__ == "__main__":
    main()