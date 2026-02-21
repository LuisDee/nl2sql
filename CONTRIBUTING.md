# Contributing

## Setup

```bash
uv sync --dev
pre-commit install
```

## Running Tests

```bash
make test                # Unit tests only
make test-integration    # Integration tests (requires live BQ + LiteLLM)
make test-cov            # Tests with coverage report (threshold: 80%)
```

## Code Style

Enforced by [ruff](https://docs.astral.sh/ruff/) via pre-commit hooks:

```bash
make lint         # Autofix lint issues + format
make type-check   # Mypy type checking
```

## Local CI

Run the full GitHub Actions workflow locally using [act](https://github.com/nektos/act):

```bash
make ci
```

## Commit Messages

Use [conventional commits](https://www.conventionalcommits.org/):

- `feat:` new feature
- `fix:` bug fix
- `chore:` maintenance (deps, config, CI)
- `docs:` documentation
- `refactor:` code restructuring (no behavior change)

## Adding a New Tool

1. Create function in `nl2sql_agent/tools/`
2. Add to tools list in `nl2sql_agent/agent.py`
3. Add progress message to `nl2sql_agent/mcp_server.py`
4. Write tests in `tests/`

## Adding a New Exchange

1. Add entry to `catalog/_exchanges.yaml` (name, aliases, dataset pair)
2. Create BQ datasets for the new exchange
3. Run `python scripts/run_embeddings.py --step all` to populate metadata

No code changes needed — the exchange registry is data-driven.
