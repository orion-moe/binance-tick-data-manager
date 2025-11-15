# Development Tools

Utility scripts for debugging, diagnostics, and testing.

## Scripts

### `diagnose_timestamps.py`
Comprehensive timestamp diagnosis tool for parquet files.
- Analyzes timestamp formats and ranges
- Detects inconsistencies and issues
- Provides detailed reports

**Usage:**
```bash
python tools/diagnose_timestamps.py
```

### `diagnose_timestamps_simple.py`
Simplified version of timestamp diagnosis.
- Quick timestamp checks
- Lightweight analysis

**Usage:**
```bash
python tools/diagnose_timestamps_simple.py
```

### `test_validation.py`
Test script for parquet file validation functions.
- Validates parquet file integrity
- Checks schema consistency

**Usage:**
```bash
python tools/test_validation.py
```

## When to Use

These tools are for **development and debugging** only:
- ✅ Investigating data issues
- ✅ Testing new features
- ✅ Debugging timestamp problems
- ❌ Not part of the production pipeline
- ❌ Not imported by main.py

## See Also

- Production scripts: `src/scripts/`
- Maintenance scripts: `src/scripts/maintenance/`
