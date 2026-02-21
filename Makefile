.PHONY: help lint format type-check test test-integration test-cov serve eval ci pre-commit clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

lint: ## Run ruff linter (with autofix)
	uv run ruff check . --fix
	uv run ruff format .

format: ## Format code with ruff
	uv run ruff format .

type-check: ## Run mypy type checker
	uv run mypy nl2sql_agent/

test: ## Run unit tests
	uv run pytest tests/ -v

test-integration: ## Run integration tests (requires live services)
	uv run pytest -m integration tests/integration/ -v

test-cov: ## Run tests with coverage report
	uv run pytest tests/ -v --cov=nl2sql_agent --cov-report=html --cov-report=term
	@echo "HTML report: htmlcov/index.html"

serve: ## Start ADK web UI
	uv run adk web --host 0.0.0.0 .

eval: ## Run offline evaluation
	uv run python eval/run_eval.py --mode offline

ci: ## Run full CI locally via act (same as GitHub Actions)
	act push

pre-commit: ## Run pre-commit on all files
	uv run pre-commit run --all-files

clean: ## Remove build artifacts and caches
	rm -rf __pycache__ .pytest_cache .mypy_cache htmlcov .coverage
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
