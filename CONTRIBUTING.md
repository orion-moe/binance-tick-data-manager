# Contributing to Crypto ML Finance Pipeline

First off, thank you for considering contributing to this project! 👍

## Code of Conduct

By participating in this project, you agree to abide by our [Code of Conduct](CODE_OF_CONDUCT.md).

## How Can I Contribute?

### 🐛 Reporting Bugs

Before creating bug reports, please check existing issues to avoid duplicates.

**How to Submit a Bug Report:**

1. Use the issue template
2. Include a clear title and description
3. Provide steps to reproduce
4. Include error messages and logs
5. Specify your environment (OS, Python version, etc.)

### 💡 Suggesting Enhancements

Enhancement suggestions are welcome! Please:

1. Check if the enhancement has already been suggested
2. Create a detailed issue describing the feature
3. Explain why this enhancement would be useful
4. Provide examples of how it would work

### 🔧 Pull Requests

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass (`pytest tests/`)
6. Format your code (`black src/`)
7. Commit your changes (`git commit -m 'Add amazing feature'`)
8. Push to your branch (`git push origin feature/amazing-feature`)
9. Open a Pull Request

## Development Setup

```bash
# Clone your fork
git clone https://github.com/yourusername/crypto-ml-finance.git
cd crypto-ml-finance

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt

# Install pre-commit hooks
pre-commit install
```

## Style Guidelines

### Python Style

We use [Black](https://github.com/psf/black) for code formatting and follow PEP 8.

```bash
# Format code
black src/ tests/

# Check style
flake8 src/ tests/

# Type checking
mypy src/
```

### Commit Messages

- Use the present tense ("Add feature" not "Added feature")
- Use the imperative mood ("Move cursor to..." not "Moves cursor to...")
- Limit the first line to 72 characters
- Reference issues and pull requests liberally

Example:
```
Add imbalance bars parameter optimization

- Implement grid search for alpha parameters
- Add validation metrics
- Update documentation

Fixes #123
```

### Documentation

- Use docstrings for all public functions
- Include type hints
- Provide examples in docstrings
- Update README.md if adding new features

```python
def calculate_imbalance(trades: pd.DataFrame, threshold: float) -> pd.DataFrame:
    """
    Calculate dollar volume imbalance for trades.

    Args:
        trades: DataFrame with columns ['price', 'qty', 'side']
        threshold: Imbalance threshold for bar creation

    Returns:
        DataFrame with imbalance bars

    Example:
        >>> trades = pd.DataFrame({'price': [100, 101], 'qty': [1, 2]})
        >>> bars = calculate_imbalance(trades, threshold=1000)
    """
```

## Testing

### Writing Tests

- Place tests in `tests/` directory
- Use pytest for testing
- Aim for >80% code coverage
- Include both unit and integration tests

```python
# tests/test_imbalance_bars.py
import pytest
from src.features.imbalance_bars import calculate_imbalance

def test_calculate_imbalance_basic():
    """Test basic imbalance calculation."""
    trades = create_sample_trades()
    bars = calculate_imbalance(trades, threshold=1000)
    assert len(bars) > 0
    assert 'imbalance' in bars.columns
```

### Running Tests

```bash
# Run all tests
pytest tests/

# Run with coverage
pytest --cov=src tests/

# Run specific test file
pytest tests/test_imbalance_bars.py
```

## Areas We Need Help

- **Performance Optimization**: Improving processing speed
- **Testing**: Increasing test coverage
- **Documentation**: Improving examples and tutorials
- **Feature Engineering**: New bar types and indicators
- **Cloud Integration**: AWS/GCP/Azure deployment guides
- **Visualization**: Dashboard and monitoring tools

## Questions?

Feel free to:
- Open an issue for questions
- Join our Discord server (coming soon)
- Email the maintainers

## Recognition

Contributors will be recognized in:
- [AUTHORS.md](AUTHORS.md) file
- Release notes
- Project documentation

Thank you for contributing! 🎉