# Contributing to filemaker-mcp

Thanks for your interest in contributing!

## Filing Issues

- Search existing issues first to avoid duplicates
- Include your FileMaker Server version and Python version
- For bugs, include steps to reproduce and the error output

## Pull Requests

1. Fork the repo and create a branch from `main`
2. Install dependencies: `uv sync`
3. Make your changes
4. Run tests: `uv run pytest -v`
5. Run linting: `uv run ruff check . && uv run ruff format --check .`
6. Submit a PR with a clear description of the change

## Code Style

- Type hints on all functions
- Docstrings on public functions (these become MCP tool descriptions)
- `async/await` throughout — httpx and FastMCP are async-native
- Format with `ruff format`, lint with `ruff check`

## Important Note

This project is maintained from an upstream source. Your PR may be rebased
or regenerated rather than merged directly. The change will be preserved,
but the git history may look different. We appreciate your contribution
regardless of how it gets integrated.

## License

By contributing, you agree that your contributions will be licensed under
the GPL-3.0 license.
