#!/usr/bin/env python3
"""
Diagnose timestamp issues in parquet files
"""
import pyarrow.parquet as pq
import pandas as pd
from pathlib import Path
import sys

def check_timestamp_format(file_path):
    """Check if timestamps are in milliseconds or microseconds"""
    try:
        # Read just the schema and first few rows
        parquet_file = pq.ParquetFile(file_path)

        # Get schema info
        schema = parquet_file.schema_arrow
        time_field = schema.field('time')

        # Read first batch
        first_batch = parquet_file.read_row_group(0, columns=['time'])
        df = first_batch.to_pandas()

        # Get raw timestamp value (before pandas conversion)
        table = parquet_file.read_row_group(0, columns=['time'])
        raw_values = table.column('time').to_pylist()[:5]

        # Get pandas converted values
        pd_values = df['time'].head()

        # Check digit count of raw values
        if raw_values:
            first_raw = raw_values[0]
            if hasattr(first_raw, 'value'):
                # It's a pandas Timestamp, get the underlying value in nanoseconds
                raw_int = first_raw.value
            else:
                raw_int = int(first_raw)

            digit_count = len(str(abs(raw_int)))

            # Determine unit based on digit count and schema
            if digit_count >= 18:  # nanoseconds
                actual_unit = 'nanoseconds'
            elif digit_count >= 15:  # microseconds
                actual_unit = 'microseconds'
            elif digit_count >= 12:  # milliseconds
                actual_unit = 'milliseconds'
            else:  # seconds
                actual_unit = 'seconds'

            return {
                'file': file_path.name,
                'schema_type': str(time_field.type),
                'digit_count': digit_count,
                'actual_unit': actual_unit,
                'first_raw_value': raw_int,
                'first_converted_date': pd_values.iloc[0],
                'last_converted_date': df['time'].iloc[-1],
                'row_count': len(df)
            }
    except Exception as e:
        return {
            'file': file_path.name,
            'error': str(e)
        }

def main():
    # Check both directories
    directories = [
        ('raw-parquet-daily', 'data/btcusdt-spot/raw-parquet-daily'),
        ('raw-parquet-merged-daily', 'data/btcusdt-spot/raw-parquet-merged-daily')
    ]

    for dir_name, dir_path in directories:
        print(f"\n{'='*80}")
        print(f"Analyzing: {dir_name}")
        print(f"Path: {dir_path}")
        print(f"{'='*80}\n")

        path = Path(dir_path)
        if not path.exists():
            print(f"❌ Directory does not exist: {path}")
            continue

        parquet_files = sorted(path.glob('*.parquet'))
        if not parquet_files:
            print(f"⚠️  No parquet files found in {path}")
            continue

        print(f"Found {len(parquet_files)} parquet files\n")

        # Sample files to check
        if dir_name == 'raw-parquet-daily':
            # Check first 5, middle 5, and last 5 files
            first_5 = list(range(min(5, len(parquet_files))))
            middle_5 = [len(parquet_files) // 2 + i for i in range(-2, 3) if 0 <= len(parquet_files) // 2 + i < len(parquet_files)]
            last_5 = list(range(max(0, len(parquet_files) - 5), len(parquet_files)))
            sample_indices = first_5 + middle_5 + last_5
            sample_files = [parquet_files[i] for i in sorted(set(sample_indices))]
            print(f"Sampling {len(sample_files)} files (first 5, middle 5, last 5)...\n")
        else:
            # Check all merged files
            sample_files = parquet_files
            print(f"Checking all {len(sample_files)} files...\n")

        # Track issues
        issues = []
        correct_files = []

        for file_path in sample_files:
            result = check_timestamp_format(file_path)

            if 'error' in result:
                print(f"❌ {result['file']}: ERROR - {result['error']}")
                issues.append(result)
            else:
                schema_unit = 'ms' if '[ms]' in result['schema_type'] else (
                    'us' if '[us]' in result['schema_type'] else
                    'ns' if '[ns]' in result['schema_type'] else 'unknown'
                )

                # Check for mismatch
                is_mismatch = False
                year = result['first_converted_date'].year

                if year > 2100 or year < 2000:
                    is_mismatch = True

                status = "⚠️  MISMATCH" if is_mismatch else "✅ OK"

                print(f"{status} {result['file']}")
                print(f"   Schema: {result['schema_type']}")
                print(f"   Digits: {result['digit_count']} ({result['actual_unit']})")
                print(f"   Date range: {result['first_converted_date'].strftime('%Y-%m-%d')} to {result['last_converted_date'].strftime('%Y-%m-%d')}")

                if is_mismatch:
                    print(f"   ⚠️  PROBLEM: Year {year} is out of range!")
                    print(f"   Raw value: {result['first_raw_value']}")
                    issues.append(result)
                else:
                    correct_files.append(result)

                print()

        # Summary
        print(f"\n{'='*80}")
        print(f"SUMMARY for {dir_name}")
        print(f"{'='*80}")
        print(f"✅ Correct files: {len(correct_files)}")
        print(f"⚠️  Files with issues: {len(issues)}")

        if issues:
            print(f"\n⚠️  FILES WITH TIMESTAMP ISSUES:")
            for issue in issues:
                if 'error' not in issue:
                    print(f"   - {issue['file']}: {issue['actual_unit']} stored as {issue['schema_type']}")
                    print(f"     Showing as: {issue['first_converted_date']}")

if __name__ == '__main__':
    main()
