#!/usr/bin/env python3
"""
Simple timestamp diagnosis
"""
import pyarrow.parquet as pq
import pandas as pd
from pathlib import Path

def check_file(file_path):
    """Check timestamps in a parquet file"""
    try:
        # Read parquet file
        parquet_file = pq.ParquetFile(file_path)

        # Get schema
        schema = parquet_file.schema_arrow
        time_field = schema.field('time')

        # Read first and last row groups
        first_batch = parquet_file.read_row_group(0, columns=['time'])
        last_batch = parquet_file.read_row_group(parquet_file.num_row_groups - 1, columns=['time'])

        first_df = first_batch.to_pandas()
        last_df = last_batch.to_pandas()

        min_date = first_df['time'].min()
        max_date = last_df['time'].max()

        # Check if dates are reasonable
        min_year = min_date.year
        max_year = max_date.year

        is_ok = 2000 <= min_year <= 2100 and 2000 <= max_year <= 2100

        return {
            'file': file_path.name,
            'schema': str(time_field.type),
            'min_date': min_date,
            'max_date': max_date,
            'min_year': min_year,
            'max_year': max_year,
            'ok': is_ok,
            'rows': parquet_file.metadata.num_rows
        }
    except Exception as e:
        return {
            'file': file_path.name,
            'error': str(e)
        }

# Check raw-parquet-daily
print("="*80)
print("Checking: data/btcusdt-spot/raw-parquet-daily")
print("="*80)

daily_path = Path("data/btcusdt-spot/raw-parquet-daily")
if daily_path.exists():
    daily_files = sorted(daily_path.glob('*.parquet'))
    print(f"Total files: {len(daily_files)}\n")

    if daily_files:
        # Sample first, middle, last
        sample_files = [
            daily_files[0],
            daily_files[len(daily_files)//2],
            daily_files[-1]
        ]

        for f in sample_files:
            result = check_file(f)
            if 'error' in result:
                print(f"❌ {result['file']}: {result['error']}")
            else:
                status = "✅" if result['ok'] else "⚠️ "
                print(f"{status} {result['file']}")
                print(f"   Schema: {result['schema']}")
                print(f"   Range: {result['min_date']} to {result['max_date']}")
                print(f"   Rows: {result['rows']:,}")
                if not result['ok']:
                    print(f"   ⚠️  YEAR OUT OF RANGE: {result['min_year']} - {result['max_year']}")
                print()
else:
    print("Directory not found or empty\n")

# Check raw-parquet-merged-daily
print("\n" + "="*80)
print("Checking: data/btcusdt-spot/raw-parquet-merged-daily")
print("="*80)

merged_path = Path("data/btcusdt-spot/raw-parquet-merged-daily")
if merged_path.exists():
    merged_files = sorted(merged_path.glob('*.parquet'))
    print(f"Total files: {len(merged_files)}\n")

    issues = []
    ok_files = []

    for f in merged_files:
        result = check_file(f)
        if 'error' in result:
            print(f"❌ {result['file']}: {result['error']}")
            issues.append(result)
        else:
            status = "✅" if result['ok'] else "⚠️ "
            print(f"{status} {result['file']}")
            print(f"   Schema: {result['schema']}")
            print(f"   Range: {result['min_date']} to {result['max_date']}")
            print(f"   Rows: {result['rows']:,}")
            if not result['ok']:
                print(f"   ⚠️  YEAR OUT OF RANGE: {result['min_year']} - {result['max_year']}")
                issues.append(result)
            else:
                ok_files.append(result)
            print()

    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"✅ OK files: {len(ok_files)}")
    print(f"⚠️  Problem files: {len(issues)}")

    if issues:
        print("\nPROBLEM FILES:")
        for issue in issues:
            if 'error' not in issue:
                print(f"  - {issue['file']}: Years {issue['min_year']}-{issue['max_year']}")
else:
    print("Directory not found\n")
