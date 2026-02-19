# Track 04: Agent Logic (Phase D) - Specification

## Overview
Implement the comprehensive system prompt, ADK callbacks, and agent configuration that transform the NL2SQL agent from disconnected tools into an intelligent, domain-expert SQL assistant for Mako Group's trading desk.

## Functional Requirements

### FR-1: Dynamic System Prompt (`prompts.py`)
- Callable `build_nl2sql_instruction(ctx: ReadonlyContext) -> str` function
- Dynamic injection of current date, project ID, and dataset names
- Complete table catalog (13 tables across 2 datasets) with routing rules
- Tool usage order (6-step pipeline)
- SQL generation rules (FQN, trade_date partition, ROUND, LIMIT)
- Clarification rules (when to ask vs proceed)
- Security constraints (read-only, no DDL/DML)

### FR-2: ADK Callbacks (`callbacks.py`)
- `before_tool_guard`: Validates SQL tool inputs, blocks DML/DDL before execution
- `after_tool_log`: Structured logging of tool results (status, row_count)

### FR-3: Agent Wiring Updates (`agent.py`)
- Replace static instruction with callable `build_nl2sql_instruction`
- Add `GenerateContentConfig(temperature=0.1)` for deterministic SQL
- Wire `before_tool_callback` and `after_tool_callback`
- Refine root agent instruction for reliable delegation

## Non-Functional Requirements

### NFR-1: Prompt Engineering Standards
- No hardcoded project IDs (use `settings.*`)
- ALWAYS/NEVER rules for SQL constraints
- Explicit routing rules as numbered list

### NFR-2: Callback Performance
- Callbacks must be lightweight (logging + simple guards only)
- No heavy processing or external calls in callbacks

### NFR-3: ADK Compatibility
- Use `ReadonlyContext` for instruction functions (not InvocationContext)
- Use `google.genai.types.GenerateContentConfig` for temperature
- Callback signatures match ADK spec exactly

## Design Decisions (Resolved)

1. **BASELINE Method**: Single comprehensive prompt, not multi-agent CHASE decomposition
2. **Callable Instruction**: Dynamic function over static string for date injection
3. **Temperature 0.1**: Per Google's ADK NL2SQL sample for deterministic SQL
4. **Defense-in-Depth**: 3-layer security (execute_sql guard + prompt constraints + callback validation)
5. **Asymmetric Embeddings**: RETRIEVAL_QUERY for search, RETRIEVAL_DOCUMENT for stored content (validated)

## Acceptance Criteria

- [ ] `prompts.py` exists with callable `build_nl2sql_instruction`
- [ ] `callbacks.py` exists with `before_tool_guard` and `after_tool_log`
- [ ] `agent.py` uses callable instruction, `GenerateContentConfig(temperature=0.1)`, and callbacks
- [ ] System prompt contains dynamic date, all 13 tables, routing rules, SQL rules, security constraints
- [ ] `before_tool_guard` blocks DML/DDL in SQL tools
- [ ] All project/dataset references use `settings.*`
- [ ] All tests pass (Tracks 01-04)

## Out of Scope
- LLM behavioral testing (Track 05)
- LoopAgent for automatic retry (future)
- Semantic caching (future)
- Model selection strategy (future)
