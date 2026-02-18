# Dependency Graph

> Directed Acyclic Graph (DAG) of track dependencies.
> Matches Phases A-F from initial-plan.md.

---

## Track Dependencies

| Track | Depends On | Interfaces Consumed |
|-------|-----------|---------------------|
| 01_foundation | — | — |
| 02_context_layer | 01_foundation | Agent Skeleton, Infra |
| 03_agent_tools | 02_context_layer | Vector Search, Metadata Loader |
| 04_agent_logic | 03_agent_tools | Dry Run, execute_sql |
| 05_eval_hardening | 04_agent_logic | End-to-end Pipeline |

---

## DAG Visualization

```
Wave 1:  [01_foundation]
              │
Wave 2:  [02_context_layer]
              │
Wave 3:  [03_agent_tools]
              │
Wave 4:  [04_agent_logic]
              │
Wave 5:  [05_eval_hardening]
```
