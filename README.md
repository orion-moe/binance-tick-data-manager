# 🚀 Crypto ML Finance Pipeline

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

A high-performance data pipeline for cryptocurrency trading data, implementing advanced financial machine learning techniques including **Imbalance Dollar Bars** based on Lopez de Prado's methodology.

## 📊 Features

- **Complete ETL Pipeline**: Download → Extract → Transform → Optimize → Validate → Feature Engineering
- **High-Performance Processing**: Leveraging Dask for distributed computing and Numba JIT compilation
- **Imbalance Dollar Bars**: Implementation of Lopez de Prado's advanced bars for ML in finance
- **Data Integrity First**: Checksum verification, validation at every step, corruption detection
- **Resume Capability**: Automatic progress tracking and recovery from interruptions
- **Multi-Market Support**: Spot, Futures (USD-M), and Futures (COIN-M) markets
- **Production Ready**: Logging, error handling, and optimized memory management

## 🏗️ Architecture

```mermaid
graph LR
    A[Binance API] --> B[Download with Checksum]
    B --> C[Extract & Validate CSV]
    C --> D[Convert to Parquet]
    D --> E[Optimize & Merge]
    E --> F[Validate Missing Data]
    F --> G[Generate Features]
    G --> H[Imbalance Dollar Bars]
```

## 🚀 Quick Start

### Prerequisites

- Python 3.8 or higher
- 16GB+ RAM recommended for large datasets
- 100GB+ free disk space for historical data

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/crypto-ml-finance.git
cd crypto-ml-finance

# Install dependencies
pip install -r requirements.txt
```

### Basic Usage

```bash
# Interactive mode (recommended for beginners)
python main.py

# Download data
python main.py download --symbol BTCUSDT --type spot --granularity daily \
    --start 2024-01-01 --end 2024-01-31

# Optimize parquet files
python main.py optimize --source datasets/dataset-raw-daily-compressed \
    --target datasets/dataset-raw-daily-compressed-optimized --max-size 10

# Generate imbalance bars
python main.py features --type imbalance
```

## 📁 Project Structure

```
crypto-ml-finance/
├── main.py                          # Main orchestrator with CLI
├── src/
│   ├── data_pipeline/
│   │   ├── downloaders/             # Binance data downloaders
│   │   ├── extractors/              # ZIP to CSV extraction
│   │   ├── converters/              # CSV to Parquet conversion
│   │   ├── processors/              # Parquet optimization & merging
│   │   └── validators/              # Data validation
│   └── features/
│       └── imbalance_bars.py        # Imbalance Dollar Bars implementation
├── datasets/                        # Data storage (auto-created)
├── output/                          # Generated features
└── requirements.txt                 # Python dependencies
```

## 🔧 Core Components

### 1. Data Download
- Downloads historical trade data from Binance
- Supports daily and monthly granularity
- Automatic checksum verification
- Resume capability with progress tracking

### 2. Data Processing
- **Integrity-First Pipeline**: Stops on data corruption
- **Automatic Format Conversion**: ZIP → CSV → Parquet
- **Memory Efficient**: Streaming processing for large files
- **Optimization**: Merges small files into optimized chunks

### 3. Feature Engineering

#### Imbalance Dollar Bars
Implementation of Lopez de Prado's advanced bar sampling technique that creates bars based on dollar volume imbalance rather than time or tick count.

**Key Benefits:**
- Better statistical properties for ML models
- More informative samples during high activity periods
- Reduced noise in low activity periods

**Algorithm Parameters:**
- `init_T`: Initial volume threshold
- `alpha_volume`: EWMA decay factor for volume
- `alpha_imbalance`: EWMA decay factor for imbalance

## 📊 Data Pipeline Workflow

### Step 1: Download Data
```bash
python main.py download --symbol BTCUSDT --type spot \
    --granularity daily --start 2024-01-01 --end 2024-12-31
```

### Step 2: Process Pipeline
The pipeline automatically:
1. Extracts CSV from ZIP files
2. Validates data integrity
3. Converts to Parquet format
4. Optimizes file sizes
5. Validates for missing dates

### Step 3: Generate Features
```bash
python main.py features --type imbalance
```

## ⚙️ Configuration

### Environment Variables
```bash
# Optional: Configure Dask settings
export DASK_WORKERS=10
export DASK_MEMORY_LIMIT="6.4GB"
```

### Pipeline Configuration
Edit `main.py` to adjust:
- Worker threads for downloads
- Maximum file sizes for optimization
- Dask cluster configuration

## 🔬 Advanced Features

### Automatic Missing Data Detection
```bash
# Add missing daily data automatically
python main.py
# Select option 7 from menu
```

### Custom Bar Generation
Modify parameters in `src/features/imbalance_bars.py`:
```python
# Volume thresholds: 1M to 40M in 5M steps
init_T0 in range(1_000_000, 40_000_000, 5_000_000)

# Alpha parameters: 0.1 to 0.9 in 0.3 steps
alpha_volume in range(10, 100, 30)
alpha_imbalance in range(10, 100, 30)
```

## 📈 Performance Optimization

### Memory Management
- **Dask**: Distributed processing for large datasets
- **Chunking**: Automatic file splitting for memory efficiency
- **Streaming**: Process data without loading entirely into memory

### Speed Optimization
- **Numba JIT**: Compiled functions for 10-100x speedup
- **Parallel Downloads**: Concurrent data fetching
- **Optimized I/O**: Parquet format for fast reads/writes

## 🐛 Troubleshooting

### Common Issues

1. **Memory Errors**
```bash
# Reduce Dask workers
export DASK_WORKERS=4
export DASK_MEMORY_LIMIT="4GB"
```

2. **Corrupt Data Files**
```bash
# Re-download specific date range
python main.py download --symbol BTCUSDT --type spot \
    --granularity daily --start 2024-01-01 --end 2024-01-01
```

3. **Missing Dependencies**
```bash
pip install --upgrade -r requirements.txt
```

## 📚 Documentation

- [Architecture Overview](docs/architecture.md)
- [API Reference](docs/api.md)
- [Feature Engineering Guide](docs/features.md)
- [MLOps Best Practices](docs/mlops.md)

## 🤝 Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development Setup
```bash
# Install dev dependencies
pip install -r requirements-dev.txt

# Run tests
pytest tests/

# Format code
black src/
```

## 📝 License

This project is licensed under the MIT License - see [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **Marcos Lopez de Prado** - For the Imbalance Bars methodology from "Advances in Financial Machine Learning"
- **Binance** - For providing historical cryptocurrency data
- **Dask & Numba** - For high-performance computing capabilities

## 📮 Contact

- **Author**: Felipe [Your Last Name]
- **Email**: your.email@example.com
- **GitHub**: [@yourusername](https://github.com/yourusername)

## ⚠️ Disclaimer

This software is for educational and research purposes only. Do not use for actual trading without proper risk management and testing. Cryptocurrency trading carries significant risk of loss.

---

Made with ❤️ for the quantitative finance community