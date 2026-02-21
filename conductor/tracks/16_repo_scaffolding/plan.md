# Track 16: Repo Scaffolding & Local CI — Implementation Plan

## Phase 1: Ruff + Mypy Configuration

### [x] Task 1.1: Add ruff configuration to pyproject.toml `2e8f0fd`

**File:** `pyproject.toml`

**Add:**
```toml
[tool.ruff]
target-version = "py311"
line-length = 88
src = ["nl2sql_agent", "tests"]

[tool.ruff.lint]
select = [
    "E",    # pycodestyle errors
    "W",    # pycodestyle warnings
    "F",    # pyflakes
    "UP",   # pyupgrade (modernize syntax)
    "B",    # flake8-bugbear (common bugs)
    "I",    # isort (import sorting)
    "N",    # pep8-naming
    "S",    # flake8-bandit (security)
    "T20",  # flake8-print (no bare print)
    "PT",   # flake8-pytest-style
    "SIM",  # flake8-simplify
    "RUF",  # ruff-specific rules
]
ignore = [
    "E501",  # line length (formatter handles this)
]

[tool.ruff.lint.per-file-ignores]
"tests/**/*.py" = ["S101"]  # allow assert in tests

[tool.ruff.lint.isort]
known-first-party = ["nl2sql_agent"]

[tool.ruff.format]
quote-style = "double"
indent-style = "space"
```

Also add ruff and mypy to dev dependencies:
```toml
[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
    "pytest-asyncio>=0.23.0",
    "pytest-cov>=5.0.0",
    "ruff>=0.9.0",
    "mypy>=1.10.0",
    "pre-commit>=3.7.0",
]
```

And add numpy to runtime dependencies (used in serialization.py but only transitively available):
```toml
dependencies = [
    ...existing...,
    "numpy>=1.24.0",
]
```

**Run:** `uv sync` to install new dev deps.

### [x] Task 1.2: Run ruff autofix pass on entire codebase `a79c4bc`

**Commands:**
```bash
ruff check . --fix        # Autofix lint issues (import sorting, pyupgrade, etc.)
ruff format .             # Format all Python files
```

**Review the diff carefully.** Ruff autofix should only make safe changes: import reordering, string quote normalization, f-string modernization, unnecessary else removal, etc.

**Run:** `pytest tests/ -v` — all 357+ tests must still pass after autofix.

**Commit:** `style: ruff lint and format pass on entire codebase`

### [x] Task 1.3: Add mypy configuration to pyproject.toml `2e8f0fd`

**File:** `pyproject.toml`

**Add:**
```toml
[tool.mypy]
python_version = "3.11"
warn_return_any = true
warn_unused_configs = true
check_untyped_defs = true
disallow_untyped_defs = false  # Start permissive; tighten later
no_implicit_optional = true

[[tool.mypy.overrides]]
module = "tests.*"
disallow_untyped_defs = false
check_untyped_defs = false

[[tool.mypy.overrides]]
module = [
    "google.adk.*",
    "google.cloud.*",
    "litellm.*",
    "structlog.*",
]
ignore_missing_imports = true
```

### [x] Task 1.4: Create py.typed marker `a469a91`

**Create:** `nl2sql_agent/py.typed` (empty file — PEP 561 marker)

### [x] Task 1.5: Run mypy and fix blocking errors `a469a91`

**Command:** `mypy nl2sql_agent/`

Expected: many warnings but hopefully zero errors with the permissive config. If there are errors:

- Fix genuine type bugs (wrong return types, missing None checks)
- Add `# type: ignore[specific-code]` for false positives with a comment explaining why
- Do NOT add `# type: ignore` without a specific error code

**Commit:** `chore: add ruff and mypy configuration, py.typed marker`

---

## Phase 2: Pre-commit Hooks

### [x] Task 2.1: Create .pre-commit-config.yaml `cc4e41c`

**Create:** `.pre-commit-config.yaml`

```yaml
repos:
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v5.0.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-yaml
      - id: check-toml
      - id: check-merge-conflict
      - id: check-added-large-files
        args: ["--maxkb=500"]
      - id: debug-statements
      - id: detect-private-key

  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.9.1
    hooks:
      - id: ruff
        args: ["--fix"]
      - id: ruff-format

  - repo: https://github.com/gitleaks/gitleaks
    rev: v8.21.2
    hooks:
      - id: gitleaks
```

**Note:** Mypy is intentionally NOT in pre-commit (too slow for every commit). It runs in `make ci` / `act` instead.

### [x] Task 2.2: Install and run pre-commit on all files `cc4e41c`

**Commands:**
```bash
uv run pre-commit install                  # Install git hooks
uv run pre-commit run --all-files          # Verify all files pass
```

Fix any issues found (trailing whitespace, end-of-file, YAML issues).

**Run:** `pytest tests/ -v` — all tests still pass.

**Commit:** `chore: add pre-commit hooks (ruff, gitleaks, file checks)`

---

## Phase 3: GitHub Actions CI + Local `act` Runner

### [x] Task 3.1: Create reusable uv setup action `5085443`

**Create:** `.github/actions/setup-uv/action.yml`

```yaml
name: "Setup UV"
description: "Install uv, Python, and sync dependencies"
runs:
  using: "composite"
  steps:
    - name: Install uv
      uses: astral-sh/setup-uv@v5
      with:
        enable-cache: true
        cache-dependency-glob: "uv.lock"
    - name: Set up Python
      run: uv python install
      shell: bash
    - name: Install dependencies
      run: uv sync --all-extras --dev
      shell: bash
```

### [x] Task 3.2: Create CI workflow `5085443`

**Create:** `.github/workflows/ci.yml`

```yaml
name: CI
on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: ./.github/actions/setup-uv
      - name: Ruff lint
        run: uv run ruff check .
      - name: Ruff format check
        run: uv run ruff format --check .

  type-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: ./.github/actions/setup-uv
      - name: Mypy
        run: uv run mypy nl2sql_agent/

  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: ./.github/actions/setup-uv
      - name: Unit tests with coverage
        run: uv run pytest tests/ -v --cov=nl2sql_agent --cov-report=xml
      - name: Check coverage threshold
        run: uv run coverage report --fail-under=80
```

**Design notes:**
- Three parallel jobs: lint, type-check, test
- No GitHub-specific secrets or caching (act-compatible)
- Coverage threshold enforced
- Only unit tests run in CI (integration tests need live services)

### [x] Task 3.3: Configure act for local CI `5085443`

**Prerequisite:** `brew install act` (user already requested this)

**Create:** `.actrc`
```
--container-architecture linux/amd64
-P ubuntu-latest=catthehacker/ubuntu:act-latest
```

**Test:** `act --list` to verify workflow parsing.

**Test:** `act push` to run the full CI workflow locally.

**Troubleshoot:**
- If `astral-sh/setup-uv@v5` doesn't work in act, fall back to `pip install uv` in the workflow
- If act can't find the composite action, use inline steps instead

**Commit:** `feat: add GitHub Actions CI workflow and local act runner`

---

## Phase 4: Makefile

### [x] Task 4.1: Create Makefile with standard targets `b8353ec`

**Create:** `Makefile`

```makefile
.PHONY: help lint format type-check test test-integration test-cov serve eval ci clean

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
```

**Commit:** `chore: add Makefile with standard dev targets`

---

## Phase 5: Root Cleanup & Developer Experience

### [x] Task 5.1: Move misplaced root files `82e41ae`

**Commands (git mv to preserve history):**
```bash
git mv ARCHITECTURE_REPORT.md docs/ARCHITECTURE_REPORT.md
git mv initial-plan.md docs/initial-plan.md
git mv raw_sql_init.sql setup/raw_sql_init.sql
```

**After moving:** Search for any markdown links referencing these files and update them.

### [x] Task 5.2: Update .gitignore `82e41ae`

**File:** `.gitignore`

**Add these entries:**
```gitignore
# Build artifacts
*.egg-info/
nl2sql_agent.egg-info/

# ADK metadata (root level)
.adk/

# Empty/generated directories
embeddings/

# Benchmarks
.benchmarks/

# Coverage
htmlcov/
.coverage
coverage.xml

# mypy
.mypy_cache/
```

**Also:** Remove tracked `.egg-info` from git:
```bash
git rm -r --cached nl2sql_agent.egg-info/
```

### [x] Task 5.3: Add coverage configuration to pyproject.toml `2e8f0fd`

**File:** `pyproject.toml`

**Add:**
```toml
[tool.coverage.run]
source = ["nl2sql_agent"]
branch = true

[tool.coverage.report]
show_missing = true
fail_under = 80
exclude_lines = [
    "pragma: no cover",
    "if TYPE_CHECKING:",
    "if __name__ == .__main__.",
]
```

### [x] Task 5.4: Create .editorconfig `82e41ae`

**Create:** `.editorconfig`

```ini
root = true

[*]
end_of_line = lf
insert_final_newline = true
trim_trailing_whitespace = true
charset = utf-8

[*.py]
indent_style = space
indent_size = 4

[*.{yml,yaml,toml}]
indent_style = space
indent_size = 2

[Makefile]
indent_style = tab
```

### [x] Task 5.5: Create .python-version `82e41ae`

**Create:** `.python-version`

```
3.11
```

This pins the Python version for `uv python install` and makes the required version explicit.

### [x] Task 5.6: Upgrade README.md `82e41ae`

**File:** `README.md`

Rewrite to include these sections:

1. **Title + one-liner** (keep existing)
2. **Architecture** — agent hierarchy diagram (text), two-layer metadata, tool pipeline
3. **Quick Start** — prerequisites (Python 3.11+, uv, gcloud ADC), install, configure, run
4. **Development** — `make help` output, how to run tests, how to lint, how to run CI locally
5. **Project Structure** — updated directory map (reflecting current state)
6. **Configuration** — table of env vars with descriptions and defaults (reference .env.example)
7. **Evaluation** — how to run offline/online eval
8. **Deployment** — Docker Compose for local, MCP server for Gemini CLI

Keep it under 150 lines. Senior engineers value concise READMEs.

### [x] Task 5.7: Create CONTRIBUTING.md `82e41ae`

**Create:** `CONTRIBUTING.md`

Short, practical guide (~50 lines):

1. **Setup** — `uv sync --dev`, `uv run pre-commit install`
2. **Running Tests** — `make test`, `make test-integration`
3. **Code Style** — `make lint` (ruff enforces style), `make type-check`
4. **Local CI** — `make ci` (runs act)
5. **Commit Messages** — conventional commits (feat/fix/chore/docs/refactor)
6. **Adding a New Tool** — create in `nl2sql_agent/tools/`, add to `agent.py` tools list, add progress message to `mcp_server.py`, write tests
7. **Adding a New Exchange** — add entry to `catalog/_exchanges.yaml`, populate BQ datasets

**Commit:** `docs: clean up root, upgrade README, add CONTRIBUTING.md`

---

## Phase 6: Final Verification

### [ ] Task 6.1: Run full local CI via act

**Command:** `make ci`

This runs `act push` which executes all three CI jobs (lint, type-check, test) inside Docker containers.

**Expected:** All three jobs pass green.

**If act fails:**
- Check `.actrc` container architecture setting
- Verify Docker is running
- Check if composite action syntax works in act (may need inline fallback)
- Verify uv installation works in act container

### [ ] Task 6.2: Run pre-commit on all files

**Command:** `make pre-commit`

**Expected:** All hooks pass (trailing whitespace, EOF, YAML, TOML, ruff, gitleaks).

### [ ] Task 6.3: Verify test suite

**Command:** `make test-cov`

**Expected:**
- 357+ tests passing
- Coverage >= 80%
- HTML report generated at `htmlcov/index.html`

### [ ] Task 6.4: Verify clean git status

```bash
git status          # No untracked junk files
git diff --stat     # Only expected changes
```

Root directory should contain ONLY:
```
.actrc
.editorconfig
.gitignore
.pre-commit-config.yaml
.python-version
AGENTS.md
CLAUDE.md
CONTRIBUTING.md
Dockerfile
Makefile
README.md
docker-compose.yml
pyproject.toml
uv.lock
```

Plus directories: `.github/`, `catalog/`, `conductor/`, `data/`, `docs/`, `eval/`, `examples/`, `nl2sql_agent/`, `scripts/`, `setup/`, `tests/`

**Commit:** `chore: verify full CI pass and clean repo state`

---

## Phase 7: Tighten to A+

This phase runs AFTER Track 15 (TypedDict contracts) is complete. With typed returns on all tools, full strict mypy becomes feasible.

### [ ] Task 7.1: Tighten mypy to strict mode

**File:** `pyproject.toml`

**Change:**
```toml
[tool.mypy]
python_version = "3.11"
strict = true
```

Remove the permissive `disallow_untyped_defs = false` from Phase 1. With Track 15's TypedDict contracts in place, most tool functions will already satisfy strict mode.

**Fix remaining errors:** Add type annotations to any untyped functions. Use `# type: ignore[specific-code]` sparingly for third-party library issues (ADK, litellm).

**Run:** `mypy nl2sql_agent/` — zero errors.

### [ ] Task 7.2: Add Python 3.12 to CI matrix

**File:** `.github/workflows/ci.yml`

**Change:** Add matrix strategy to the test job:

```yaml
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.11", "3.12"]
    steps:
      - uses: actions/checkout@v4
      - uses: ./.github/actions/setup-uv
        with:
          python-version: ${{ matrix.python-version }}
      - name: Unit tests with coverage
        run: uv run pytest tests/ -v --cov=nl2sql_agent --cov-report=xml
```

Update the composite action to accept a `python-version` input:
```yaml
inputs:
  python-version:
    description: "Python version to install"
    required: false
    default: "3.11"
```

**Test:** `act push` — both Python versions pass.

### [ ] Task 7.3: Remove sample_data.tar.gz from tracking

The 116MB archive should not be in git. Options:
- (a) Add to `.gitignore` and `git rm --cached sample_data.tar.gz`
- (b) Move to git-lfs: `git lfs track "*.tar.gz"`

**Chosen approach (a):** Simple removal. The archive is regeneratable from `sample_data/` (already gitignored).

```bash
echo "sample_data.tar.gz" >> .gitignore
git rm --cached sample_data.tar.gz
```

### [ ] Task 7.4: Document intentional YAML design choice

**File:** `catalog/README.md` (or create if it doesn't exist)

Add a section explaining why KPI YAML files are self-contained (not deduplicated):

> **Design Decision: Self-Contained Table YAMLs**
>
> Each table YAML file contains ALL columns for that table, including shared columns
> that appear across multiple KPI tables. This is intentional:
>
> - Each file is independently editable — changing a column description for one table
>   doesn't affect others
> - Columns can diverge between tables over time (different descriptions, different
>   business meanings)
> - No risk of shared reference breaking multiple files
> - Embedding generation treats each file as a standalone unit
>
> The trade-off is file size (~200-376KB per KPI file). This is acceptable because
> these files are read by code (not humans) and cached in memory.

This turns what looks like a code smell into a documented architectural choice.

**Commit:** `chore: tighten mypy strict, multi-version CI, document catalog design`

---

## Summary

| Phase | Tasks | Key Outcome |
|-------|-------|-------------|
| 1 | 1.1–1.5 | Ruff + mypy configured and passing |
| 2 | 2.1–2.2 | Pre-commit hooks enforcing on every commit |
| 3 | 3.1–3.3 | GitHub Actions CI + local act runner |
| 4 | 4.1 | Makefile with `make ci`, `make test`, `make lint` |
| 5 | 5.1–5.7 | Clean root, polished README, CONTRIBUTING.md |
| 6 | 6.1–6.4 | Full CI pass verified locally via act |
| 7 | 7.1–7.4 | Strict mypy, multi-version CI, large file cleanup, documented design |

## Verification Commands

```bash
# Phase 1: Linting passes
make lint
make type-check

# Phase 2: Pre-commit passes
make pre-commit

# Phase 3: Local CI passes
make ci

# Phase 4: Makefile works
make help

# Phase 5: Root is clean
ls -1 *.md *.toml *.yml *.lock Makefile Dockerfile 2>/dev/null

# Phase 6: Full green
make test-cov

# Phase 7: Strict mypy
mypy --strict nl2sql_agent/
```
