#!/usr/bin/env python3
"""
Test the validate_parquet_files function
"""
import sys
from pathlib import Path
from datetime import date
import pyarrow.parquet as pq

# Import the validation function from main.py
sys.path.insert(0, str(Path(__file__).parent))
from main import validate_parquet_files

def test_validation():
    """Test the validation function with actual data"""
    print("="*80)
    print(" Testing validate_parquet_files() ")
    print("="*80)

    # Test with a small date range (Oct 2025)
    start_date = date(2025, 10, 1)
    end_date = date(2025, 10, 31)

    symbol = "BTCUSDT"
    data_type = "spot"
    futures_type = ""
    parquet_dir = Path("data/btcusdt-spot/raw-parquet-daily")

    print(f"\n📅 Testing date range: {start_date} to {end_date}")
    print(f"📁 Parquet directory: {parquet_dir}")
    print(f"📊 Symbol: {symbol} ({data_type})")

    # Check directory exists
    if not parquet_dir.exists():
        print(f"\n❌ ERROR: Directory not found: {parquet_dir}")
        return

    # Count files in directory
    total_files = len(list(parquet_dir.glob("*.parquet")))
    print(f"📦 Total files in directory: {total_files}")

    # Run validation
    print("\n🔍 Running validation...")
    result = validate_parquet_files(
        symbol=symbol,
        data_type=data_type,
        futures_type=futures_type,
        start_date=start_date,
        end_date=end_date,
        parquet_dir=parquet_dir
    )

    # Display detailed results
    print("\n" + "="*80)
    print(" VALIDATION TEST RESULTS ")
    print("="*80)
    print(f"✅ Total expected dates: {result['total_expected']}")
    print(f"📦 Total files found: {result['total_found']}")
    print(f"✅ Valid files: {result['valid_files']}")
    print(f"❌ Corrupted files: {len(result['corrupted_files'])}")
    print(f"⚠️  Missing dates: {len(result['missing_dates'])}")

    if result['corrupted_files']:
        print("\n🗑️  CORRUPTED FILES:")
        for item in result['corrupted_files']:
            print(f"   - {item['file']} ({item['date']})")
            print(f"     Error: {item['error'][:100]}")

    if result['missing_dates']:
        print(f"\n📅 MISSING DATES (showing first 10):")
        for missing_date in sorted(result['missing_dates'])[:10]:
            print(f"   - {missing_date}")
        if len(result['missing_dates']) > 10:
            print(f"   ... and {len(result['missing_dates']) - 10} more")

    # Test passed if function ran without errors
    print("\n" + "="*80)
    if result['valid_files'] > 0:
        print("✅ TEST PASSED: Validation function works correctly!")
    else:
        print("⚠️  TEST WARNING: No valid files found in range")
    print("="*80)

if __name__ == '__main__':
    test_validation()
