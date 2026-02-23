# Track 16: Repo Scaffolding & Local CI

## Problem

The NL2SQL agent has solid architecture (protocol DI, structured logging, pydantic config, clean module separation) but zero enforcement tooling. There is:

- **No linter or formatter** — no ruff, black, flake8, or isort config anywhere
- **No type checker** — no mypy or pyright config, no `py.typed` marker
- **No pre-commit hooks** — no `.pre-commit-config.yaml`, no secret scanning
- **No CI pipeline** — no `.github/workflows/`, no automated checks on any push or PR
- **No task runner** — no Makefile or justfile; common commands undocumented
- **No coverage config** — no `fail_under` threshold, no branch coverage
- **Messy root directory** — `ARCHITECTURE_REPORT.md` (55KB), `initial-plan.md` (49KB), `raw_sql_init.sql`, empty `embeddings/` dir, `.egg-info/` tracked
- **Minimal README** — no architecture overview, no development instructions, no quick-start for contributors
- **Missing dev dependencies** — ruff, mypy, pre-commit, pytest-cov not in `[project.optional-dependencies]`
- **Undeclared runtime dependency** — numpy used in serialization.py but only available transitively

A senior Python ML engineer opening this repo would see good code wrapped in amateur infrastructure. This track closes that gap with a local-first CI approach using `act` (GitHub Actions runner in Docker).

## Solution

A 6-phase track that scaffolds modern Python tooling, enforced both locally (pre-commit + `act`) and remotely (GitHub Actions):

- **Phase 1**: Ruff + mypy configuration in pyproject.toml, initial codebase lint/format pass
- **Phase 2**: Pre-commit hooks (ruff, gitleaks, file checks)
- **Phase 3**: GitHub Actions CI workflow + local `act` runner
- **Phase 4**: Makefile with standardized dev commands
- **Phase 5**: Root cleanup, README upgrade, developer experience polish
- **Phase 6**: Final verification — full local CI pass via `act`

### Local CI Strategy (`act`)

Instead of relying solely on GitHub-hosted runners, the CI workflow runs locally via `act` (nektos/act) in Docker. This gives:

- **Fast feedback** — run full CI before pushing, not after
- **Offline capable** — no GitHub dependency for basic checks
- **Same workflow** — `.github/workflows/ci.yml` runs identically local and remote
- **Pre-commit for fast checks** — ruff + gitleaks on every commit (seconds)
- **`make ci` for full validation** — runs `act` for lint + type-check + test (minutes, before push)

Workflow: `git commit` triggers pre-commit (fast) → `make ci` triggers `act` (thorough) → `git push` triggers GitHub Actions (authoritative).

## Scope

### In Scope

- Ruff lint + format config (pyproject.toml `[tool.ruff]`)
- Mypy config (pyproject.toml `[tool.mypy]`)
- `py.typed` PEP 561 marker
- `.pre-commit-config.yaml` (ruff, gitleaks, file checks)
- `.github/workflows/ci.yml` (lint, type-check, test — parallel jobs)
- `.github/actions/setup-uv/action.yml` (reusable composite)
- `act` configuration (`.actrc`) and Makefile `ci` target
- `Makefile` with lint, type-check, test, serve, ci targets
- `.editorconfig`, `.python-version`
- Root file cleanup (move misplaced files, update .gitignore)
- README.md upgrade (architecture, development, quick-start sections)
- `CONTRIBUTING.md`
- Coverage config (`[tool.coverage.run]`, `[tool.coverage.report]`)
- Dev dependency declarations (ruff, mypy, pre-commit, pytest-cov)
- Add numpy to runtime dependencies

### Out of Scope

- `src/` layout migration (ADK's `adk web` expects flat layout; not worth the disruption)
- KPI YAML deduplication (separate large-scope track)
- Dockerfile / Docker fixes (Track 13 scope)
- `.dockerignore` (Track 13 scope)
- `.env.example` (Track 15 scope)
- ADRs / architecture decision records (nice-to-have, not blocking)
- MkDocs / API documentation generation (separate track if needed)

## Relationship to Other Tracks

| Track | Overlap | Resolution |
|-------|---------|------------|
| Track 13 (Autopsy Fixes) | Dockerfile fix, .dockerignore, lazy BQ init | Track 13 owns Docker; Track 16 does NOT touch Docker |
| Track 15 (Code Quality) | .env.example, TypedDict types | Track 15 owns .env.example and types.py; Track 16 references them |
| Track 16 (this) | Ruff may surface new warnings after Track 13/15 changes | Track 16 should run AFTER Track 13 and 15 for cleanest result |

**Recommended execution order**: Track 13 → Track 15 → Track 16 (or Track 15 ∥ Track 16 if careful about merge conflicts in pyproject.toml).

## Key Design Decisions

### 1. Ruff rule selection

Start with a practical rule set, not `select = ["ALL"]`. Target rules that catch real bugs and enforce consistency without being noisy:

```
E, W (pycodestyle), F (pyflakes), UP (pyupgrade), B (bugbear),
I (isort), N (pep8-naming), S (bandit security), T20 (no print),
PT (pytest style), SIM (simplify), RUF (ruff-specific)
```

Ignore `S101` (assert) in tests. This matches what FastAPI, pydantic, and pandas use.

### 2. Mypy strictness level

Start with `warn_return_any = true` and `check_untyped_defs = true` but NOT `strict = true`. The codebase has no type annotations on many functions (especially tools). Going full strict would require annotating everything at once. Instead:

- Enable incrementally: warn on obvious issues now
- Track 15 adds TypedDict contracts which will satisfy mypy for tool returns
- Tighten to `strict = true` later when coverage is higher

### 3. Act configuration

Use `act` with `--container-architecture linux/amd64` on Apple Silicon. Use the medium Ubuntu image (`catthehacker/ubuntu:act-latest`) for speed. The CI workflow must avoid GitHub-only features (secrets, deployments, caching) to remain `act`-compatible.

### 4. Makefile over justfile

Makefile requires no installation (`make` ships with macOS/Linux). `justfile` is nicer but adds a dependency. For an internal team tool, Makefile is the pragmatic choice.

### 5. Root cleanup strategy

Move files to appropriate directories, update any internal references, commit the moves as renames (git tracks them). Don't delete — just relocate.

## Files Created

| File | Phase | Purpose |
|------|-------|---------|
| `.github/workflows/ci.yml` | 3 | CI pipeline (lint, type-check, test) |
| `.github/actions/setup-uv/action.yml` | 3 | Reusable uv setup composite |
| `.pre-commit-config.yaml` | 2 | Pre-commit hook definitions |
| `.actrc` | 3 | Default act configuration |
| `Makefile` | 4 | Standardized dev commands |
| `.editorconfig` | 5 | Cross-editor formatting |
| `.python-version` | 5 | Pin Python version for uv |
| `CONTRIBUTING.md` | 5 | Developer onboarding guide |
| `nl2sql_agent/py.typed` | 1 | PEP 561 type marker |

## Files Modified

| File | Phase | Change |
|------|-------|--------|
| `pyproject.toml` | 1, 1, 4 | Add [tool.ruff], [tool.mypy], [tool.coverage.*], dev deps, numpy |
| `README.md` | 5 | Architecture, development, quick-start sections |
| `.gitignore` | 5 | Add .egg-info/, .benchmarks/, .adk/, embeddings/ |
| All `.py` files | 1 | Ruff autofix pass (import sorting, pyupgrade, style) |

## Files Moved (git mv)

| From | To | Reason |
|------|-----|--------|
| `ARCHITECTURE_REPORT.md` | `docs/ARCHITECTURE_REPORT.md` | Not a root-level file |
| `initial-plan.md` | `docs/initial-plan.md` | Historical planning doc |
| `raw_sql_init.sql` | `setup/raw_sql_init.sql` | SQL belongs with other setup scripts |

## Acceptance Criteria

1. `ruff check .` passes with zero errors
2. `ruff format --check .` passes (all files formatted)
3. `mypy nl2sql_agent/` passes with zero errors (at configured strictness)
4. `pre-commit run --all-files` passes
5. `make ci` runs `act` successfully (lint + type-check + test all green)
6. `make test` runs 357+ tests, all passing
7. `py.typed` exists in `nl2sql_agent/`
8. Root directory has no misplaced files
9. README.md contains Architecture, Development, and Quick Start sections
10. CONTRIBUTING.md exists with setup/test/lint instructions
11. `.github/workflows/ci.yml` is valid (act parses it)
12. Coverage configured with `fail_under = 80`
13. `mypy --strict nl2sql_agent/` passes (Phase 7, after Track 15)
14. CI tests both Python 3.11 and 3.12
15. `sample_data.tar.gz` removed from git tracking
16. `catalog/README.md` documents self-contained YAML design choice

## Post-Implementation Assessment: Is the Repo Clean?

After Tracks 13 + 15 + 16 are all complete, the repository state would be:

### What's Solved

| Category | Before | After |
|----------|--------|-------|
| Linting | None | Ruff enforced (pre-commit + CI) |
| Type checking | None | Mypy configured, py.typed marker |
| CI/CD | None | GitHub Actions + local act |
| Pre-commit | None | ruff, gitleaks, file checks |
| Task runner | None | Makefile with standard targets |
| Coverage | None | 80% threshold, branch coverage |
| Root cleanliness | 6 misplaced files | Clean: only config + README |
| README | Minimal | Architecture + Dev + Quick Start |
| Developer onboarding | Missing | CONTRIBUTING.md + .env.example |
| Docker | Broken | Fixed, non-root, .dockerignore (Track 13) |
| Type contracts | Ad-hoc dicts | TypedDict for all tools (Track 15) |
| Critical bugs | 10 critical | All fixed (Track 13) |
| Exchange cache | Pollutable | Exchange-aware (Track 15) |

### What Remains (Acceptable Deferrals)

| Item | Why It's OK to Defer |
|------|---------------------|
| `src/` layout | ADK's `adk web` command expects flat layout; both layouts are valid per Python Packaging Authority |
| KPI YAML dedup (20K lines) | Data quality issue, not code structure; large scope warrants its own track |
| Routing fragmentation | Track 13's YAML auto-discovery partially addresses; full unification is a design project |
| ADRs | conductor/architect/ docs serve similar purpose; ADRs are nice-to-have for an internal tool |
| MkDocs API docs | No external consumers; AGENTS.md system provides AI-optimized docs |
| Full mypy strict mode | Incremental adoption is standard practice; strict can be a future tightening |

### Verdict

After Track 13 + 15 + 16 (Phases 1-6): **A-**. Good architecture with proper tooling.

After Track 16 Phase 7 + Track 17: **A/A+**. Strict types, multi-version CI, routing consolidated, pipeline tested, and the one thing that looked like copy-paste (KPI YAMLs) is documented as an intentional design choice rather than an oversight.

## Dependencies

- Track 13 (Autopsy Fixes) — should complete first (Docker fixes, .dockerignore)
- Track 15 (Code Quality) — should complete first (.env.example, TypedDict types)
- Both can run in parallel with each other, but Track 16 should run last for cleanest lint pass

## Risks

| Risk | Mitigation |
|------|------------|
| Ruff autofix may introduce regressions | Run full test suite after autofix; review diff |
| Mypy may flag hundreds of errors | Start with permissive config; don't block on full strict |
| `act` may not support all GitHub Actions features | Keep CI workflow simple; avoid GitHub-only features (secrets, caching) |
| Pre-commit hooks slow down commits | ruff is <1s; gitleaks is <2s; mypy can be moved to `act` only if too slow |
| Root file moves may break internal links | Search for references before moving; update any markdown links |
