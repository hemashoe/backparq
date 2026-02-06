# Contributing

## Setup

```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone and install
git clone https://github.com/hemashoe/backparq.git
cd backparq
uv venv && source .venv/bin/activate
uv pip install -e ".[dev]"
```

## Tests

```bash
# Unit tests
uv run pytest tests/ -v --ignore=tests/test_integration.py

# All tests (requires Docker)
uv run pytest tests/ -v

# Linting
uv run ruff check src/ tests/

# Type checking
uv run mypy src/
```

## Code Style

- Use [ruff](https://github.com/astral-sh/ruff) for linting and formatting
- Add type hints to public functions
- Keep docstrings for public APIs

## Pull Requests

1. Fork the repository
2. Create a branch (`git checkout -b fix/your-fix`)
3. Make changes and run tests
4. Run `uv run ruff check src/ tests/`
5. Open a pull request

Look for issues labeled `good first issue` or `help wanted`.
