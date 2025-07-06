#!/usr/bin/env python3
"""
Re-optimize existing parquet files to properly reach 10GB target size using streaming.
This version uses memory-efficient streaming to handle large files without loading everything into memory.
"""

import sys
from pathlib import Path
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime
import shutil
import logging
import gc

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_file_info(file_path: Path) -> dict:
    """Get file information including size and row count"""
    try:
        size_gb = file_path.stat().st_size / (1024**3)
        # Get row count without loading full file
        parquet_file = pq.ParquetFile(file_path)
        row_count = parquet_file.metadata.num_rows
        return {
            'path': file_path,
            'size_gb': size_gb,
            'rows': row_count
        }
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        return None

def estimate_rows_per_gb(file_infos):
    """Estimate average rows per GB from existing files"""
    total_rows = sum(info['rows'] for info in file_infos)
    total_gb = sum(info['size_gb'] for info in file_infos)
    return int(total_rows / total_gb) if total_gb > 0 else 1_000_000

def streaming_merge_files(source_files, output_path, target_size_gb, rows_per_gb):
    """
    Merge multiple parquet files using streaming to minimize memory usage.
    Returns actual size in GB and row count.
    """
    writer = None
    total_rows = 0
    current_size_gb = 0
    
    try:
        for source_file in source_files:
            logger.info(f"  Processing {source_file.name}...")
            
            # Open parquet file for streaming
            parquet_file = pq.ParquetFile(source_file)
            
            # Get schema from first file
            if writer is None:
                schema = parquet_file.schema_arrow
                writer = pq.ParquetWriter(output_path, schema, compression='snappy')
            
            # Process file in batches
            for batch in parquet_file.iter_batches(batch_size=1_000_000):
                # Write batch
                writer.write_batch(batch)
                total_rows += len(batch)
                
                # Estimate current size
                current_size_gb = total_rows / rows_per_gb
                
                # Check if we're approaching target size
                if current_size_gb >= target_size_gb * 0.95:
                    logger.info(f"    Reached target size (~{current_size_gb:.1f} GB)")
                    writer.close()
                    actual_size = output_path.stat().st_size / (1024**3)
                    return actual_size, total_rows, source_files[:source_files.index(source_file)+1]
                
                # Free memory periodically
                if total_rows % 10_000_000 == 0:
                    gc.collect()
                    logger.info(f"    Processed {total_rows:,} rows (~{current_size_gb:.1f} GB)")
        
        # Close writer
        if writer:
            writer.close()
        
        actual_size = output_path.stat().st_size / (1024**3)
        return actual_size, total_rows, source_files
        
    except Exception as e:
        logger.error(f"Error during streaming merge: {e}")
        if writer:
            writer.close()
        raise

def reoptimize_directory_streaming(data_dir: Path, target_size_gb: float = 10.0, dry_run: bool = False):
    """Re-optimize parquet files using memory-efficient streaming"""
    
    # Find all optimized parquet files
    pattern = "*-Trades-Optimized-*.parquet"
    files = sorted(data_dir.glob(pattern))
    
    if not files:
        logger.info(f"No optimized parquet files found in {data_dir}")
        return
    
    logger.info(f"Found {len(files)} optimized parquet files")
    
    # Get info for all files
    file_infos = []
    total_size = 0
    total_rows = 0
    
    for f in files:
        info = get_file_info(f)
        if info:
            file_infos.append(info)
            total_size += info['size_gb']
            total_rows += info['rows']
            logger.info(f"  {f.name}: {info['size_gb']:.2f} GB, {info['rows']:,} rows")
    
    logger.info(f"\nTotal: {total_size:.2f} GB, {total_rows:,} rows")
    
    # Calculate rows per GB for estimation
    rows_per_gb = estimate_rows_per_gb(file_infos)
    logger.info(f"Estimated rows per GB: {rows_per_gb:,}")
    
    # Check if re-optimization is needed
    avg_size = total_size / len(file_infos) if file_infos else 0
    if avg_size >= target_size_gb * 0.9:
        logger.info("Files are already close to target size. No re-optimization needed.")
        return
    
    # Calculate new file distribution
    estimated_files_needed = int(total_size / (target_size_gb * 0.95)) + 1
    logger.info(f"\nRe-optimization plan:")
    logger.info(f"  Current files: {len(file_infos)}")
    logger.info(f"  Target files: {estimated_files_needed}")
    logger.info(f"  Target size per file: ~{target_size_gb * 0.95:.1f} GB")
    
    if dry_run:
        logger.info("\nDRY RUN - No changes will be made")
        return
    
    # Confirm with user
    confirm = input(f"\nProceed with re-optimization? (yes/no): ").strip().lower()
    if confirm != 'yes':
        logger.info("Re-optimization cancelled")
        return
    
    # Create backup directory
    backup_dir = data_dir.parent / f"{data_dir.name}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    backup_dir.mkdir(exist_ok=True)
    logger.info(f"\nBacking up files to: {backup_dir}")
    
    # Backup all files first
    for info in file_infos:
        src = info['path']
        dst = backup_dir / src.name
        logger.info(f"  Backing up {src.name}...")
        shutil.copy2(src, dst)
    
    # Get symbol from first file name
    first_file = file_infos[0]['path'].name
    symbol = first_file.split('-Trades-')[0]
    
    logger.info(f"\nRe-optimizing files for {symbol} using streaming...")
    
    # Process files using streaming
    remaining_files = [info['path'] for info in file_infos]
    output_file_num = 1
    files_created = 0
    temp_files = []
    
    while remaining_files:
        output_path = data_dir / f"{symbol}-Trades-Optimized-{output_file_num:03d}.parquet.new"
        logger.info(f"\nCreating {output_path.name}...")
        
        try:
            # Merge files using streaming until target size is reached
            actual_size, rows_written, files_used = streaming_merge_files(
                remaining_files, 
                output_path, 
                target_size_gb,
                rows_per_gb
            )
            
            logger.info(f"  Created: {actual_size:.2f} GB with {rows_written:,} rows")
            temp_files.append(output_path)
            files_created += 1
            output_file_num += 1
            
            # Remove processed files from remaining list
            for used_file in files_used:
                if used_file in remaining_files:
                    remaining_files.remove(used_file)
            
            # Force garbage collection
            gc.collect()
            
        except Exception as e:
            logger.error(f"Error creating {output_path.name}: {e}")
            # Clean up temp files on error
            for temp_file in temp_files:
                if temp_file.exists():
                    temp_file.unlink()
            raise
    
    # Replace old files with new ones
    logger.info(f"\nReplacing old files with new optimized files...")
    
    try:
        # First, delete old files
        for info in file_infos:
            info['path'].unlink()
            logger.info(f"  Deleted {info['path'].name}")
        
        # Then rename new files
        for temp_file in temp_files:
            final_name = temp_file.with_suffix('')  # Remove .new
            temp_file.rename(final_name)
            logger.info(f"  Renamed {temp_file.name} to {final_name.name}")
        
        logger.info(f"\n✅ Re-optimization completed!")
        logger.info(f"  Files created: {files_created}")
        logger.info(f"  Backup location: {backup_dir}")
        
        # Show new file sizes
        logger.info(f"\nNew file sizes:")
        new_files = sorted(data_dir.glob(pattern))
        for f in new_files:
            size_gb = f.stat().st_size / (1024**3)
            logger.info(f"  {f.name}: {size_gb:.2f} GB")
            
    except Exception as e:
        logger.error(f"Error during file replacement: {e}")
        # Try to restore from backup
        logger.info("Attempting to restore from backup...")
        for info in file_infos:
            backup_file = backup_dir / info['path'].name
            if backup_file.exists() and not info['path'].exists():
                shutil.copy2(backup_file, info['path'])
                logger.info(f"  Restored {info['path'].name}")
        raise

def main():
    print("\n🔧 Parquet File Re-Optimizer (Streaming Version)")
    print("=" * 50)
    print("This tool re-optimizes existing parquet files to reach the 10GB target size")
    print("Uses memory-efficient streaming to handle large files")
    print("It will create a backup before making any changes")
    
    # Get market type
    print("\nSelect market type:")
    print("1. Spot")
    print("2. Futures (USD-M)")
    print("3. Futures (COIN-M)")
    
    choice = input("\nEnter choice (1-3): ").strip()
    
    if choice == '1':
        market_type = 'spot'
    elif choice == '2':
        market_type = 'futures-um'
    elif choice == '3':
        market_type = 'futures-cm'
    else:
        print("❌ Invalid choice")
        return
    
    # Determine directory
    base_dir = Path("datasets")
    data_dir = base_dir / "dataset-raw-daily-compressed-optimized" / market_type
    
    if not data_dir.exists():
        print(f"❌ Directory not found: {data_dir}")
        return
    
    print(f"\n📁 Directory: {data_dir}")
    
    # Ask for target size
    target_input = input("\nTarget file size in GB (default: 10): ").strip()
    target_size = float(target_input) if target_input else 10.0
    
    # Ask for dry run
    dry_run = input("\nDry run? (yes/no, default: no): ").strip().lower() == 'yes'
    
    # Run re-optimization
    try:
        reoptimize_directory_streaming(data_dir, target_size, dry_run)
    except KeyboardInterrupt:
        print("\n\n⚠️  Operation cancelled by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()