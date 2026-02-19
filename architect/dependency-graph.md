# Dependency Graph

> Directed Acyclic Graph (DAG) of track dependencies.
> Last synced: 2026-02-19

---

## Track Dependencies

| Track | Depends On | Status |
|-------|-----------|--------|
| 01_foundation | — | completed |
| 02_context_layer | 01_foundation | completed |
| 03_agent_tools | 02_context_layer | completed |
| 04_agent_logic | 03_agent_tools | completed |
| 05_eval_hardening | 04_agent_logic | completed |
| 06_metadata_enrichment | 02_context_layer | completed |
| 07_dependency_fix | — | completed |
| 08_loop_and_performance_fix | 04_agent_logic, 05_eval_hardening | new |

---

## DAG Visualization

```
Wave 1:  [01_foundation]    [07_dependency_fix]
              │
Wave 2:  [02_context_layer]
              │
              ├──────────────────────┐
Wave 3:  [03_agent_tools]    [06_metadata_enrichment]
              │
Wave 4:  [04_agent_logic]
              │
Wave 5:  [05_eval_hardening]
              │
Wave 6:  [08_loop_and_performance_fix]
```

Note: Tracks within the same wave are independent and can run in parallel.
Track 06 ran in parallel with Tracks 03-04.
Track 07 was a hotfix with no dependencies (ran independently of main chain).
