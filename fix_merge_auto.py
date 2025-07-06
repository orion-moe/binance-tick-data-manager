#!/usr/bin/env python3
"""
Automatically fix undersized optimized files by re-merging them to ~10GB
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
                              start_number: int = 1) -> int:
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


def fix_futures_um():
    """Fix futures/um optimized files to reach 10GB"""
    
    symbol = "BTCUSDT"
    base_dir = Path("datasets")
    
    optimized_dir = base_dir / "dataset-raw-daily-compressed-optimized" / "futures-um"
    daily_dir = base_dir / "dataset-raw-daily-compressed" / "futures-um"
    
    logger.info(f"=== FIXING UNDERSIZED OPTIMIZED FILES FOR {symbol} futures/um ===")
    
    # Find all optimized files
    optimized_files = sorted(optimized_dir.glob(f"{symbol}-Trades-Optimized-*.parquet"))
    
    if not optimized_files:
        logger.error(f"No optimized files found in {optimized_dir}")
        return
    
    # Analyze current files
    logger.info(f"\nAnalyzing {len(optimized_files)} optimized files...")
    
    for file in optimized_files:
        size_gb = file.stat().st_size / (1024**3)
        logger.info(f"{file.name}: {size_gb:.2f} GB")
    
    # Create backup directory
    backup_dir = optimized_dir.parent / f"{optimized_dir.name}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    backup_dir.mkdir(exist_ok=True)
    
    # Backup all optimized files
    logger.info(f"\nBacking up files to {backup_dir}...")
    for file in optimized_files:
        shutil.copy2(file, backup_dir)
    logger.success(f"✅ Backed up {len(optimized_files)} files")
    
    # Get all daily files
    daily_files = sorted(daily_dir.glob(f"{symbol}-Trades-*.parquet"))
    logger.info(f"\nFound {len(daily_files)} daily files total")
    
    # Delete all existing optimized files to start fresh
    logger.info("\nDeleting existing optimized files...")
    for file in optimized_files:
        file.unlink()
    
    # Re-merge all files with proper size management
    logger.info("\nStarting fresh re-merge with 10GB target...")
    
    files_created = merge_files_to_target_size(
        daily_files, optimized_dir, symbol, start_number=1
    )
    
    logger.success(f"\n✅ Re-merge complete! Created {files_created} optimized files")
    
    # Show final status
    logger.info("\n=== FINAL STATUS ===")
    new_optimized_files = sorted(optimized_dir.glob(f"{symbol}-Trades-Optimized-*.parquet"))
    
    total_size = 0
    for file in new_optimized_files:
        size_gb = file.stat().st_size / (1024**3)
        total_size += size_gb
        logger.info(f"{file.name}: {size_gb:.2f} GB")
    
    logger.info(f"\nTotal: {len(new_optimized_files)} files, {total_size:.2f} GB")


if __name__ == "__main__":
    fix_futures_um()