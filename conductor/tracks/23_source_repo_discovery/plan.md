# Plan: Source Repo Discovery & Profiling Framework

## Phase 1: Repo Profiling

### Task 1.1: Profile CPP repo
- [x] Run sub-agent to explore `repos/cpp/`: directory tree (top 3 levels), README, build configs, `source/pb/` structure
- [x] Generate `repos/cpp/AGENTS.md` repo card with all required sections (Purpose, Tech Stack, Directory Map, Key Entry Points, Data Models, Common Patterns, Gotchas, When to Use)
- [x] Identify which proto files under `source/pb/` correspond to our BQ tables (markettrade, quotertrade, brokertrade, clicktrade, otoswing, theodata, marketdata, marketdepth, swingdata)
- [x] Document in repo card: which proto messages map to which tables, which fields are subset of the full proto

### Task 1.2: Profile data-library repo
- [x] Run sub-agent to explore `repos/data-library/`: directory tree, README, Go modules, config files
- [x] Generate `repos/data-library/AGENTS.md` repo card
- [x] Identify how proto messages are consumed: deserialization handlers, field mapping logic, any renames or type conversions at the Go layer

### Task 1.3: Profile data-loader repo
- [x] Run sub-agent to explore `repos/data-loader/`: directory tree, README, Python/SQL files, config
- [x] Generate `repos/data-loader/AGENTS.md` repo card
- [x] Identify transformation scripts: which files handle which tables, where column renames/casts/derivations happen

### Task 1.4: Profile KPI repo
- [x] Run sub-agent to explore `repos/kpi/`: directory tree, README, Python/SQL files, config
- [x] Generate `repos/kpi/AGENTS.md` repo card
- [x] Identify computation logic: which files compute which KPI columns, where formulas for instant_edge, instant_pnl, delta_slippage_* are defined

### Task 1.5: Phase 1 checkpoint
- [x] All 4 repo cards complete and reviewed
- [ ] Human verification: repo cards accurately describe each repo's purpose and structure

---

## Phase 2: Structural Indexing (Deterministic Extraction)

### Task 2.1: Extract proto field definitions
- [ ] Write validation tests for `metadata/proto_fields.yaml` schema (messages have name, file, fields; fields have name, type, number)
- [ ] Parse all `.proto` files in `repos/cpp/source/pb/` — extract message names, field names, types, field numbers, comments
- [ ] Output to `metadata/proto_fields.yaml`
- [ ] Map proto message names to BQ table names (convention-based + manual review)

### Task 2.2: Extract data-loader transformations
- [ ] Write validation tests for `metadata/data_loader_transforms.yaml` schema
- [ ] Identify and parse transformation SQL/Python in `repos/data-loader/`
- [ ] For each target table: document source columns, renames, type casts, derived columns
- [ ] Output to `metadata/data_loader_transforms.yaml`

### Task 2.3: Extract KPI computations
- [ ] Write validation tests for `metadata/kpi_computations.yaml` schema
- [ ] Identify and parse KPI computation SQL/Python in `repos/kpi/`
- [ ] For each KPI column: document formula/SQL expression, input columns, aggregation type
- [ ] Output to `metadata/kpi_computations.yaml`

### Task 2.4: Phase 2 checkpoint
- [ ] All 3 structural index files exist and pass validation tests
- [ ] Human verification: spot-check 5 proto fields, 5 transformations, 5 KPI formulas against source code

---

## Phase 3: Cross-Repo Routing & Lineage

### Task 3.1: Create cross-repo routing guide
- [ ] Write `metadata/ROUTING.md` mapping data concepts to repo locations
- [ ] Include: by-data-concept routing, by-BQ-table routing, cross-repo dependency chain
- [ ] Verify all BQ tables are covered (5 KPI + 8 data = 13 tables)

### Task 3.2: Build field lineage map
- [ ] Write validation tests for `metadata/field_lineage.yaml` schema
- [ ] For key columns (instant_edge, instant_pnl, instant_pnl_w_fees, delta_slippage_*, trade_date, event_timestamp_ns, mid_price, trade_price, signed_delta), trace: proto field → silver column → gold column + formula
- [ ] Cross-reference against proto_fields.yaml, data_loader_transforms.yaml, kpi_computations.yaml
- [ ] Output to `metadata/field_lineage.yaml`

### Task 3.3: Phase 3 checkpoint
- [ ] All metadata files complete: proto_fields.yaml, data_loader_transforms.yaml, kpi_computations.yaml, ROUTING.md, field_lineage.yaml
- [ ] Run full test suite
- [ ] Human review: verify routing guide is accurate, lineage traces are correct
