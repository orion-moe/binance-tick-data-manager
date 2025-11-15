#!/usr/bin/env python3
"""
Find and delete corrupted parquet files
"""
import pyarrow.parquet as pq
from pathlib import Path
import sys

def check_and_clean_parquets(directory):
    """Check all parquet files and delete corrupted ones"""
    parquet_dir = Path(directory)

    if not parquet_dir.exists():
        print(f"❌ Directory not found: {directory}")
        return

    parquet_files = sorted(parquet_dir.glob("*.parquet"))

    if not parquet_files:
        print(f"⚠️  No parquet files found in {directory}")
        return

    print(f"📊 Checking {len(parquet_files)} parquet files...")
    print()

    corrupted = []
    valid = []

    for i, file_path in enumerate(parquet_files, 1):
        try:
            # Try to open the file
            parquet_file = pq.ParquetFile(file_path)
            # Try to read metadata
            _ = parquet_file.metadata
            valid.append(file_path)
            if i % 50 == 0:
                print(f"✅ Checked {i}/{len(parquet_files)} files...")
        except Exception as e:
            corrupted.append((file_path, str(e)))
            print(f"❌ CORRUPTED: {file_path.name}")
            print(f"   Error: {str(e)[:100]}")

    print()
    print("="*80)
    print(" 📋 SUMMARY ")
    print("="*80)
    print(f"✅ Valid files: {len(valid)}")
    print(f"❌ Corrupted files: {len(corrupted)}")

    if corrupted:
        print()
        print("🗑️  CORRUPTED FILES TO DELETE:")
        for file_path, error in corrupted:
            file_size_mb = file_path.stat().st_size / (1024 * 1024)
            print(f"   - {file_path.name} ({file_size_mb:.1f} MB)")

        print()
        confirm = input("Delete all corrupted files? (yes/no): ").strip().lower()

        if confirm == 'yes':
            for file_path, _ in corrupted:
                file_path.unlink()
                print(f"🗑️  Deleted: {file_path.name}")
            print(f"\n✅ Deleted {len(corrupted)} corrupted files")
        else:
            print("❌ Operation cancelled")
    else:
        print("\n✅ All files are valid!")

if __name__ == '__main__':
    # Check raw-parquet-daily
    print("="*80)
    print(" Checking: data/btcusdt-spot/raw-parquet-daily/")
    print("="*80)
    check_and_clean_parquets("data/btcusdt-spot/raw-parquet-daily")

    print("\n" + "="*80)
    print(" Checking: data/btcusdt-spot/raw-parquet-merged-daily/")
    print("="*80)
    check_and_clean_parquets("data/btcusdt-spot/raw-parquet-merged-daily")
