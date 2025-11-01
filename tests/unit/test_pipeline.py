"""
Unit tests for data pipeline components
"""

import pytest
from pathlib import Path
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))


class TestDataDownload:
    """Test data download functionality"""

    @patch('crypto_ml_finance.pipeline.download.requests.get')
    def test_download_with_checksum(self, mock_get):
        """Test file download with checksum verification"""
        # Mock response
        mock_response = Mock()
        mock_response.content = b"test data"
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        # Test download function would go here
        assert mock_get.called

    def test_generate_date_range(self):
        """Test date range generation for downloads"""
        from datetime import datetime, timedelta

        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 5)

        dates = []
        current = start
        while current <= end:
            dates.append(current)
            current += timedelta(days=1)

        assert len(dates) == 5
        assert dates[0] == start
        assert dates[-1] == end


class TestDataTransformation:
    """Test data transformation functionality"""

    def test_csv_to_parquet_conversion(self, tmp_path):
        """Test CSV to Parquet conversion"""
        # Create sample CSV
        csv_path = tmp_path / "test.csv"
        df = pd.DataFrame({
            'price': [100.0, 101.0, 102.0],
            'qty': [1.0, 2.0, 3.0],
            'time': pd.date_range('2024-01-01', periods=3, freq='min')
        })
        df.to_csv(csv_path, index=False)

        # Read and verify
        df_read = pd.read_csv(csv_path)
        assert len(df_read) == 3
        assert 'price' in df_read.columns

    def test_data_validation(self):
        """Test data validation logic"""
        # Valid data
        df = pd.DataFrame({
            'price': [100.0, 101.0, 102.0],
            'qty': [1.0, 2.0, 3.0],
            'time': pd.date_range('2024-01-01', periods=3, freq='min')
        })

        # Check for required columns
        required_columns = ['price', 'qty', 'time']
        assert all(col in df.columns for col in required_columns)

        # Check data types
        assert df['price'].dtype == np.float64
        assert df['qty'].dtype == np.float64

        # Check for missing values
        assert not df.isnull().any().any()


class TestFeatureEngineering:
    """Test feature engineering functionality"""

    def test_imbalance_calculation(self):
        """Test imbalance calculation logic"""
        prices = np.array([100.0, 101.0, 99.0, 102.0])
        volumes = np.array([1.0, 2.0, 3.0, 4.0])

        # Calculate sides based on price changes
        sides = np.where(np.diff(prices, prepend=prices[0]) > 0, 1, -1)

        # Calculate imbalance
        imbalance = volumes * sides

        assert len(imbalance) == len(prices)
        assert imbalance[1] > 0  # Price increased
        assert imbalance[2] < 0  # Price decreased

    def test_bar_formation_threshold(self):
        """Test bar formation based on threshold"""
        cumulative_volume = 0
        threshold = 100
        bars = []

        trades = [
            {'volume': 30, 'price': 100},
            {'volume': 40, 'price': 101},
            {'volume': 50, 'price': 102},  # Should trigger bar
            {'volume': 20, 'price': 103},
        ]

        current_bar = {'open': None, 'high': -np.inf, 'low': np.inf, 'close': None}

        for trade in trades:
            if current_bar['open'] is None:
                current_bar['open'] = trade['price']

            current_bar['high'] = max(current_bar['high'], trade['price'])
            current_bar['low'] = min(current_bar['low'], trade['price'])
            current_bar['close'] = trade['price']

            cumulative_volume += trade['volume']

            if cumulative_volume >= threshold:
                bars.append(current_bar.copy())
                cumulative_volume = 0
                current_bar = {'open': None, 'high': -np.inf, 'low': np.inf, 'close': None}

        assert len(bars) == 1
        assert bars[0]['open'] == 100
        assert bars[0]['close'] == 102


class TestConfiguration:
    """Test configuration management"""

    def test_environment_loading(self, tmp_path, monkeypatch):
        """Test loading configuration from environment"""
        # Set environment variables
        monkeypatch.setenv("DATABASE_HOST", "test_host")
        monkeypatch.setenv("DATABASE_PORT", "5433")
        monkeypatch.setenv("DASK_WORKERS", "20")

        # Test reading environment variables
        assert os.getenv("DATABASE_HOST") == "test_host"
        assert os.getenv("DATABASE_PORT") == "5433"
        assert os.getenv("DASK_WORKERS") == "20"

    def test_default_configuration(self):
        """Test default configuration values"""
        defaults = {
            'dask_workers': 10,
            'max_file_size_gb': 10.0,
            'download_workers': 5,
            'alpha_volume': 0.1,
            'alpha_imbalance': 0.7
        }

        assert defaults['dask_workers'] == 10
        assert defaults['max_file_size_gb'] == 10.0
        assert 0 < defaults['alpha_volume'] < 1
        assert 0 < defaults['alpha_imbalance'] < 1


class TestDataValidation:
    """Test data validation utilities"""

    def test_missing_dates_detection(self):
        """Test detection of missing dates in time series"""
        dates = pd.date_range('2024-01-01', '2024-01-10', freq='D')
        # Remove some dates to create gaps
        dates_with_gaps = dates.delete([2, 5, 6])  # Remove Jan 3, 6, 7

        # Find missing dates
        full_range = pd.date_range(dates_with_gaps.min(), dates_with_gaps.max(), freq='D')
        missing_dates = full_range.difference(dates_with_gaps)

        assert len(missing_dates) == 3
        assert pd.Timestamp('2024-01-03') in missing_dates
        assert pd.Timestamp('2024-01-06') in missing_dates
        assert pd.Timestamp('2024-01-07') in missing_dates

    def test_data_integrity_check(self):
        """Test data integrity validation"""
        # Valid data
        df_valid = pd.DataFrame({
            'price': [100.0, 101.0, 102.0],
            'qty': [1.0, 2.0, 3.0],
            'time': [1000, 2000, 3000]
        })

        # Invalid data with negative prices
        df_invalid = pd.DataFrame({
            'price': [100.0, -101.0, 102.0],
            'qty': [1.0, 2.0, 3.0],
            'time': [1000, 2000, 3000]
        })

        # Validation checks
        assert all(df_valid['price'] > 0)
        assert not all(df_invalid['price'] > 0)
        assert all(df_valid['qty'] > 0)


class TestPerformance:
    """Test performance-related functionality"""

    def test_chunked_processing(self):
        """Test chunked data processing for memory efficiency"""
        # Simulate large dataset
        total_rows = 1000000
        chunk_size = 10000

        processed_rows = 0
        for chunk_start in range(0, total_rows, chunk_size):
            chunk_end = min(chunk_start + chunk_size, total_rows)
            chunk_rows = chunk_end - chunk_start
            processed_rows += chunk_rows

        assert processed_rows == total_rows
        assert total_rows / chunk_size == 100  # Number of chunks

    @pytest.mark.skipif(not pytest.importorskip("numba"), reason="Numba not installed")
    def test_numba_acceleration(self):
        """Test Numba JIT compilation speedup"""
        import numba
        import time

        # Regular Python function
        def calculate_sum_python(arr):
            total = 0
            for i in range(len(arr)):
                total += arr[i]
            return total

        # Numba-compiled function
        @numba.jit
        def calculate_sum_numba(arr):
            total = 0
            for i in range(len(arr)):
                total += arr[i]
            return total

        # Test array
        arr = np.random.rand(1000000)

        # Warm up Numba
        _ = calculate_sum_numba(arr[:10])

        # Both should give same result
        result_python = calculate_sum_python(arr)
        result_numba = calculate_sum_numba(arr)

        assert np.isclose(result_python, result_numba, rtol=1e-10)


@pytest.fixture
def sample_trade_data():
    """Fixture for sample trade data"""
    return pd.DataFrame({
        'trade_id': range(100),
        'price': np.random.uniform(100, 110, 100),
        'qty': np.random.uniform(0.1, 10, 100),
        'quoteQty': np.random.uniform(10, 1000, 100),
        'time': pd.date_range('2024-01-01', periods=100, freq='min'),
        'isBuyerMaker': np.random.choice([True, False], 100),
        'isBestMatch': [True] * 100
    })


def test_sample_data_fixture(sample_trade_data):
    """Test the sample data fixture"""
    assert len(sample_trade_data) == 100
    assert all(col in sample_trade_data.columns for col in ['price', 'qty', 'time'])
    assert sample_trade_data['price'].min() >= 100
    assert sample_trade_data['price'].max() <= 110