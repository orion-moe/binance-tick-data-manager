#!/usr/bin/env python3
"""
Script to continue merging spot data into optimized files
Specifically handles the missing days from 2024-11-28 onwards
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
from loguru import logger
import sys
import gc

# Configure logging
logger.remove()
logger.add(sys.stdout, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>")


def get_last_date_optimized(file_path: Path):
    """Get the last date from an optimized file efficiently"""
    pf = pq.ParquetFile(file_path)
    last_group = pf.read_row_group(pf.num_row_groups - 1, columns=['time'])
    df_last = last_group.to_pandas()
    
    # Handle different timestamp formats
    sample_time = df_last['time'].iloc[0]
    if isinstance(sample_time, pd.Timestamp):
        last_timestamp = df_last['time'].max()
    else:
        # Convert based on timestamp length
        if len(str(sample_time)) == 16:
            last_timestamp = pd.to_datetime(df_last['time'].max(), unit='us')
        elif len(str(sample_time)) == 13:
            last_timestamp = pd.to_datetime(df_last['time'].max(), unit='ms')
        else:
            last_timestamp = pd.to_datetime(df_last['time'].max(), unit='s')
    
    return last_timestamp.date()


def create_new_optimized_file(data: pd.DataFrame, optimized_dir: Path, symbol: str, file_number: int):
    """Create a new optimized file"""
    output_path = optimized_dir / f"{symbol}-Trades-Optimized-{file_number:03d}.parquet"
    
    logger.info(f"Creating new file: {output_path.name}")
    table = pa.Table.from_pandas(data)
    pq.write_table(table, output_path, compression='snappy')
    
    size_gb = output_path.stat().st_size / (1024**3)
    logger.success(f"✅ Created {output_path.name} ({size_gb:.2f} GB) with {len(data):,} rows")
    
    return output_path


def process_remaining_files():
    """Process the remaining files creating new optimized files"""
    
    # Configuration
    symbol = "BTCUSDT"
    base_dir = Path("datasets")
    optimized_dir = base_dir / "dataset-raw-daily-compressed-optimized" / "spot"
    daily_dir = base_dir / "dataset-raw-daily-compressed" / "spot"
    
    # Check last optimized file
    optimized_files = sorted(optimized_dir.glob(f"{symbol}-Trades-Optimized-*.parquet"))
    if not optimized_files:
        logger.error("No optimized files found!")
        return
    
    last_optimized = optimized_files[-1]
    last_number = int(last_optimized.stem.split('-')[-1])
    
    # Get last processed date
    last_date = get_last_date_optimized(last_optimized)
    logger.info(f"Last processed date: {last_date}")
    logger.info(f"Last optimized file: {last_optimized.name}")
    
    # Find unprocessed files
    daily_files = []
    pattern = f"{symbol}-Trades-*.parquet"
    
    for file in sorted(daily_dir.glob(pattern)):
        try:
            date_str = file.stem.split('-Trades-')[-1]
            file_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            
            if file_date > last_date:
                daily_files.append(file)
        except:
            continue
    
    if not daily_files:
        logger.info("✅ All files have been processed!")
        return
    
    logger.info(f"Files to process: {len(daily_files)}")
    logger.info(f"Period: {daily_files[0].name} to {daily_files[-1].name}")
    
    # Process in groups to create new optimized files
    batch_size = 30  # ~30 days per optimized file (aiming for ~10GB files)
    current_file_number = last_number + 1
    total_processed = 0
    
    for i in range(0, len(daily_files), batch_size):
        batch_files = daily_files[i:i + batch_size]
        
        logger.info(f"\n=== Processing batch {(i//batch_size) + 1} ===")
        logger.info(f"Files: {len(batch_files)}")
        logger.info(f"From: {batch_files[0].name}")
        logger.info(f"To: {batch_files[-1].name}")
        
        try:
            # Read batch files
            dfs = []
            total_rows = 0
            
            for j, file in enumerate(batch_files):
                if j % 10 == 0:
                    logger.info(f"  Reading file {j+1}/{len(batch_files)}...")
                
                df = pd.read_parquet(file)
                dfs.append(df)
                total_rows += len(df)
                
                # Free memory periodically
                if j % 20 == 0:
                    gc.collect()
            
            logger.info(f"  Total rows read: {total_rows:,}")
            
            # Combine and sort
            logger.info("  Combining data...")
            combined_df = pd.concat(dfs, ignore_index=True)
            del dfs  # Free memory
            gc.collect()
            
            logger.info("  Sorting by time...")
            combined_df = combined_df.sort_values('time').reset_index(drop=True)
            
            # Create new optimized file
            create_new_optimized_file(
                combined_df, 
                optimized_dir, 
                symbol, 
                current_file_number
            )
            
            current_file_number += 1
            total_processed += len(batch_files)
            
            # Free memory
            del combined_df
            gc.collect()
            
            logger.info(f"  Progress: {total_processed}/{len(daily_files)} files processed")
            
        except Exception as e:
            logger.error(f"❌ Error processing batch: {e}")
            logger.info(f"Processed so far: {total_processed} files")
            raise
    
    logger.success(f"\n🎉 PROCESSING COMPLETE!")
    logger.info(f"Total daily files processed: {total_processed}")
    logger.info(f"New optimized files created: {current_file_number - last_number - 1}")
    
    # List all optimized files
    final_optimized = sorted(optimized_dir.glob(f"{symbol}-Trades-Optimized-*.parquet"))
    logger.info(f"\nTotal optimized files: {len(final_optimized)}")
    
    total_size = 0
    for f in final_optimized:
        size = f.stat().st_size / (1024**3)
        total_size += size
        if f.name.endswith(f"{last_number:03d}.parquet") or int(f.stem.split('-')[-1]) > last_number:
            logger.info(f"  - {f.name}: {size:.2f} GB")
    
    logger.info(f"\nTotal size: {total_size:.2f} GB")


if __name__ == "__main__":
    logger.info("=== CONTINUING SPOT DATA OPTIMIZED FILES PROCESSING ===")
    process_remaining_files()