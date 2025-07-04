#!/usr/bin/env python3
"""
Comprehensive Data Integrity Validator
Advanced validation system with corruption detection and data quality analysis
"""

import os
import sys
import json
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from numba import njit
from concurrent.futures import ThreadPoolExecutor, as_completed


@dataclass
class DataQualityMetrics:
    """Metrics for data quality assessment"""
    file_path: str
    rows: int
    columns: int
    file_size_mb: float
    null_count: int
    duplicate_count: int
    price_anomalies: int
    volume_anomalies: int
    time_gaps: int
    data_density: float
    compression_ratio: float
    checksum: str
    is_valid: bool
    errors: List[str]
    warnings: List[str]


@dataclass
class ValidationReport:
    """Comprehensive validation report"""
    validation_time: str
    total_files: int
    valid_files: int
    invalid_files: int
    total_rows: int
    total_size_gb: float
    data_quality_score: float
    critical_errors: List[str]
    warnings: List[str]
    file_metrics: List[DataQualityMetrics]
    summary_stats: Dict[str, Any]


@njit
def detect_price_anomalies(prices: np.ndarray, threshold: float = 0.5) -> int:
    """Detect price anomalies using Numba for performance"""
    anomalies = 0
    for i in range(1, len(prices)):
        change = abs((prices[i] - prices[i-1]) / prices[i-1])
        if change > threshold:
            anomalies += 1
    return anomalies


@njit
def detect_volume_anomalies(volumes: np.ndarray, multiplier: float = 100.0) -> int:
    """Detect volume anomalies using Numba"""
    if len(volumes) < 10:
        return 0
    
    median_vol = np.median(volumes)
    anomalies = 0
    
    for vol in volumes:
        if vol > median_vol * multiplier or vol <= 0:
            anomalies += 1
    
    return anomalies


class DataIntegrityValidator:
    """
    Comprehensive data integrity validator for Bitcoin trading data
    """
    
    def __init__(self, base_dir: str = ".", log_level: int = logging.INFO):
        self.base_dir = Path(base_dir)
        self.setup_logging(log_level)
        
        # Validation thresholds
        self.thresholds = {
            'max_price_change': 0.5,  # 50% max price change
            'max_volume_multiplier': 100.0,  # Volume anomaly threshold
            'max_time_gap_minutes': 60,  # Max gap between trades
            'min_data_density': 0.01,  # Minimum data density
            'min_compression_ratio': 0.1,  # Minimum compression efficiency
            'max_null_percentage': 0.01,  # Max 1% null values
            'max_duplicate_percentage': 0.001  # Max 0.1% duplicates
        }
        
        # Expected schema for Bitcoin trading data
        self.expected_schema = {
            'trade_id': pa.int64(),
            'price': pa.float64(),
            'qty': pa.float64(),
            'quoteQty': pa.float64(),
            'time': pa.timestamp('ns'),
            'isBuyerMaker': pa.bool_(),
            'isBestMatch': pa.bool_()
        }
        
        self.logger.info("DataIntegrityValidator initialized")
        
    def setup_logging(self, log_level: int):
        """Setup comprehensive logging"""
        log_file = self.base_dir / "data_integrity_validation.log"
        
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        
        # File handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        
        # Logger
        self.logger = logging.getLogger(f"{__name__}.{id(self)}")
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def calculate_file_checksum(self, file_path: Path) -> str:
        """Calculate SHA256 checksum for file"""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
    
    def validate_schema(self, table: pa.Table) -> Tuple[bool, List[str]]:
        """Validate parquet table schema"""
        errors = []
        
        # Check required columns
        required_columns = set(self.expected_schema.keys())
        actual_columns = set(table.column_names)
        
        missing_columns = required_columns - actual_columns
        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")
        
        # Check column types for existing columns
        for col_name in required_columns.intersection(actual_columns):
            expected_type = self.expected_schema[col_name]
            actual_type = table.schema.field(col_name).type
            
            # Allow some type flexibility
            if not self._types_compatible(expected_type, actual_type):
                errors.append(f"Column '{col_name}' has incompatible type: expected {expected_type}, got {actual_type}")
        
        return len(errors) == 0, errors
    
    def _types_compatible(self, expected: pa.DataType, actual: pa.DataType) -> bool:
        """Check if types are compatible"""
        # Exact match
        if expected == actual:
            return True
        
        # Numeric type flexibility
        if pa.types.is_integer(expected) and pa.types.is_integer(actual):
            return True
        if pa.types.is_floating(expected) and pa.types.is_floating(actual):
            return True
        if pa.types.is_temporal(expected) and pa.types.is_temporal(actual):
            return True
        
        return False
    
    def validate_data_quality(self, df: pd.DataFrame, file_path: str) -> DataQualityMetrics:
        """Comprehensive data quality validation"""
        errors = []
        warnings = []
        
        # Basic metrics
        rows = len(df)
        columns = len(df.columns)
        
        # Null value analysis
        null_count = df.isnull().sum().sum()
        null_percentage = (null_count / (rows * columns)) * 100 if rows > 0 else 0
        
        if null_percentage > self.thresholds['max_null_percentage'] * 100:
            errors.append(f"High null percentage: {null_percentage:.2f}%")
        
        # Duplicate analysis
        duplicate_count = df.duplicated().sum()
        duplicate_percentage = (duplicate_count / rows) * 100 if rows > 0 else 0
        
        if duplicate_percentage > self.thresholds['max_duplicate_percentage'] * 100:
            warnings.append(f"High duplicate percentage: {duplicate_percentage:.2f}%")
        
        # Price anomaly detection
        price_anomalies = 0
        if 'price' in df.columns and len(df) > 1:
            prices = df['price'].values.astype(np.float64)
            price_anomalies = detect_price_anomalies(prices, self.thresholds['max_price_change'])
            
            if price_anomalies > 0:
                anomaly_rate = (price_anomalies / len(df)) * 100
                if anomaly_rate > 1.0:  # More than 1% anomalies
                    warnings.append(f"High price anomaly rate: {anomaly_rate:.2f}% ({price_anomalies} anomalies)")
        
        # Volume anomaly detection
        volume_anomalies = 0
        if 'qty' in df.columns and len(df) > 10:
            volumes = df['qty'].values.astype(np.float64)
            volume_anomalies = detect_volume_anomalies(volumes, self.thresholds['max_volume_multiplier'])
            
            if volume_anomalies > 0:
                anomaly_rate = (volume_anomalies / len(df)) * 100
                if anomaly_rate > 0.1:  # More than 0.1% anomalies
                    warnings.append(f"Volume anomalies detected: {anomaly_rate:.2f}% ({volume_anomalies} anomalies)")
        
        # Time gap analysis
        time_gaps = 0
        if 'time' in df.columns and len(df) > 1:
            time_diffs = df['time'].diff().dt.total_seconds() / 60  # Minutes
            large_gaps = time_diffs > self.thresholds['max_time_gap_minutes']
            time_gaps = large_gaps.sum()
            
            if time_gaps > 0:
                max_gap = time_diffs.max()
                warnings.append(f"Large time gaps detected: {time_gaps} gaps, max gap: {max_gap:.1f} minutes")
        
        # Data density calculation
        if 'time' in df.columns and len(df) > 1:
            time_span = (df['time'].max() - df['time'].min()).total_seconds() / 3600  # Hours
            data_density = len(df) / time_span if time_span > 0 else 0
        else:
            data_density = 0
        
        # File metrics
        file_size_mb = Path(file_path).stat().st_size / (1024 * 1024) if Path(file_path).exists() else 0
        memory_usage_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
        compression_ratio = file_size_mb / memory_usage_mb if memory_usage_mb > 0 else 0
        
        # Checksum
        checksum = self.calculate_file_checksum(Path(file_path)) if Path(file_path).exists() else ""
        
        # Overall validity
        is_valid = len(errors) == 0
        
        return DataQualityMetrics(
            file_path=file_path,
            rows=rows,
            columns=columns,
            file_size_mb=file_size_mb,
            null_count=null_count,
            duplicate_count=duplicate_count,
            price_anomalies=price_anomalies,
            volume_anomalies=volume_anomalies,
            time_gaps=time_gaps,
            data_density=data_density,
            compression_ratio=compression_ratio,
            checksum=checksum,
            is_valid=is_valid,
            errors=errors,
            warnings=warnings
        )
    
    def validate_single_file(self, file_path: Path) -> DataQualityMetrics:
        """Validate a single parquet file"""
        try:
            self.logger.debug(f"Validating file: {file_path}")
            
            # Check file existence
            if not file_path.exists():
                return DataQualityMetrics(
                    file_path=str(file_path),
                    rows=0, columns=0, file_size_mb=0,
                    null_count=0, duplicate_count=0,
                    price_anomalies=0, volume_anomalies=0,
                    time_gaps=0, data_density=0,
                    compression_ratio=0, checksum="",
                    is_valid=False,
                    errors=["File does not exist"],
                    warnings=[]
                )
            
            # Read parquet file
            table = pq.read_table(file_path)
            
            # Schema validation
            schema_valid, schema_errors = self.validate_schema(table)
            
            # Convert to pandas for detailed analysis
            df = table.to_pandas()
            
            # Data quality validation
            metrics = self.validate_data_quality(df, str(file_path))
            
            # Add schema errors
            if not schema_valid:
                metrics.errors.extend(schema_errors)
                metrics.is_valid = False
            
            self.logger.debug(f"Validation complete: {file_path.name} - Valid: {metrics.is_valid}")
            return metrics
            
        except Exception as e:
            self.logger.error(f"Failed to validate {file_path}: {e}")
            return DataQualityMetrics(
                file_path=str(file_path),
                rows=0, columns=0, file_size_mb=0,
                null_count=0, duplicate_count=0,
                price_anomalies=0, volume_anomalies=0,
                time_gaps=0, data_density=0,
                compression_ratio=0, checksum="",
                is_valid=False,
                errors=[f"Validation error: {str(e)}"],
                warnings=[]
            )
    
    def validate_directory(self, directory: Path, max_workers: int = 4) -> ValidationReport:
        """Validate all parquet files in a directory"""
        self.logger.info(f"Validating directory: {directory}")
        
        # Find all parquet files
        parquet_files = list(directory.glob("*.parquet"))
        
        if not parquet_files:
            self.logger.warning(f"No parquet files found in {directory}")
            return ValidationReport(
                validation_time=datetime.now().isoformat(),
                total_files=0, valid_files=0, invalid_files=0,
                total_rows=0, total_size_gb=0.0,
                data_quality_score=0.0,
                critical_errors=["No parquet files found"],
                warnings=[], file_metrics=[],
                summary_stats={}
            )
        
        self.logger.info(f"Found {len(parquet_files)} parquet files")
        
        # Validate files in parallel
        file_metrics = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {
                executor.submit(self.validate_single_file, file_path): file_path
                for file_path in parquet_files
            }
            
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    metrics = future.result()
                    file_metrics.append(metrics)
                    
                    if metrics.is_valid:
                        self.logger.info(f"✅ {file_path.name}: {metrics.rows:,} rows")
                    else:
                        self.logger.error(f"❌ {file_path.name}: {len(metrics.errors)} errors")
                        
                except Exception as e:
                    self.logger.error(f"Failed to process {file_path}: {e}")
        
        # Generate report
        return self._generate_report(file_metrics)
    
    def _generate_report(self, file_metrics: List[DataQualityMetrics]) -> ValidationReport:
        """Generate comprehensive validation report"""
        # Basic statistics
        total_files = len(file_metrics)
        valid_files = sum(1 for m in file_metrics if m.is_valid)
        invalid_files = total_files - valid_files
        
        total_rows = sum(m.rows for m in file_metrics)
        total_size_gb = sum(m.file_size_mb for m in file_metrics) / 1024
        
        # Collect all errors and warnings
        critical_errors = []
        all_warnings = []
        
        for metrics in file_metrics:
            critical_errors.extend(metrics.errors)
            all_warnings.extend(metrics.warnings)
        
        # Calculate data quality score (0-100)
        if total_files > 0:
            quality_score = (valid_files / total_files) * 100
            
            # Adjust for warnings
            warning_penalty = min(len(all_warnings) * 2, 20)  # Max 20% penalty
            quality_score = max(0, quality_score - warning_penalty)
        else:
            quality_score = 0
        
        # Summary statistics
        summary_stats = {
            'avg_file_size_mb': sum(m.file_size_mb for m in file_metrics) / total_files if total_files > 0 else 0,
            'avg_rows_per_file': total_rows / total_files if total_files > 0 else 0,
            'total_null_count': sum(m.null_count for m in file_metrics),
            'total_duplicates': sum(m.duplicate_count for m in file_metrics),
            'total_price_anomalies': sum(m.price_anomalies for m in file_metrics),
            'total_volume_anomalies': sum(m.volume_anomalies for m in file_metrics),
            'total_time_gaps': sum(m.time_gaps for m in file_metrics),
            'avg_compression_ratio': sum(m.compression_ratio for m in file_metrics) / total_files if total_files > 0 else 0
        }
        
        return ValidationReport(
            validation_time=datetime.now().isoformat(),
            total_files=total_files,
            valid_files=valid_files,
            invalid_files=invalid_files,
            total_rows=total_rows,
            total_size_gb=total_size_gb,
            data_quality_score=quality_score,
            critical_errors=critical_errors,
            warnings=all_warnings,
            file_metrics=file_metrics,
            summary_stats=summary_stats
        )
    
    def save_report(self, report: ValidationReport, output_path: Path):
        """Save validation report to JSON file"""
        report_dict = asdict(report)
        
        with open(output_path, 'w') as f:
            json.dump(report_dict, f, indent=2, default=str)
        
        self.logger.info(f"Report saved to: {output_path}")
    
    def print_report_summary(self, report: ValidationReport):
        """Print a summary of the validation report"""
        print("\n" + "="*60)
        print("DATA INTEGRITY VALIDATION REPORT")
        print("="*60)
        print(f"Validation Time: {report.validation_time}")
        print(f"Total Files: {report.total_files}")
        print(f"Valid Files: {report.valid_files}")
        print(f"Invalid Files: {report.invalid_files}")
        print(f"Total Rows: {report.total_rows:,}")
        print(f"Total Size: {report.total_size_gb:.2f} GB")
        print(f"Data Quality Score: {report.data_quality_score:.1f}/100")
        
        if report.invalid_files > 0:
            print(f"\n❌ Critical Errors ({len(report.critical_errors)}):")
            for error in report.critical_errors[:10]:  # Show first 10
                print(f"  - {error}")
            if len(report.critical_errors) > 10:
                print(f"  ... and {len(report.critical_errors) - 10} more")
        
        if report.warnings:
            print(f"\n⚠️  Warnings ({len(report.warnings)}):")
            for warning in report.warnings[:10]:  # Show first 10
                print(f"  - {warning}")
            if len(report.warnings) > 10:
                print(f"  ... and {len(report.warnings) - 10} more")
        
        print(f"\n📊 Summary Statistics:")
        for key, value in report.summary_stats.items():
            if isinstance(value, float):
                print(f"  {key}: {value:.2f}")
            else:
                print(f"  {key}: {value:,}")
        
        if report.data_quality_score >= 95:
            print("\n✅ Excellent data quality!")
        elif report.data_quality_score >= 80:
            print("\n✅ Good data quality")
        elif report.data_quality_score >= 60:
            print("\n⚠️  Acceptable data quality with some issues")
        else:
            print("\n❌ Poor data quality - review required")


def main():
    """Main function for command line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Comprehensive data integrity validation for parquet files"
    )
    parser.add_argument('--directory', required=True, help='Directory containing parquet files')
    parser.add_argument('--output', help='Output path for validation report JSON')
    parser.add_argument('--max-workers', type=int, default=4, help='Maximum worker threads')
    parser.add_argument('--verbose', action='store_true', help='Verbose logging')
    
    args = parser.parse_args()
    
    # Setup logging level
    log_level = logging.DEBUG if args.verbose else logging.INFO
    
    # Create validator
    validator = DataIntegrityValidator(log_level=log_level)
    
    # Run validation
    directory = Path(args.directory)
    report = validator.validate_directory(directory, args.max_workers)
    
    # Print summary
    validator.print_report_summary(report)
    
    # Save detailed report if requested
    if args.output:
        output_path = Path(args.output)
        validator.save_report(report, output_path)
    
    # Exit with appropriate code
    sys.exit(0 if report.invalid_files == 0 else 1)


if __name__ == "__main__":
    main()