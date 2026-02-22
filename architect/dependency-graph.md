# Dependency Graph

> Directed Acyclic Graph (DAG) of track dependencies.
> Last synced: 2026-02-21

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
| 08_loop_and_performance_fix | 04_agent_logic, 05_eval_hardening | completed |
| 09_production_hardening | 08_loop_and_performance_fix | completed |
| 10_metadata_gaps | 09_production_hardening, 06_metadata_enrichment | completed |
| 11_gemini_cli_mcp | 09_production_hardening | completed |
| 12_column_semantic_search | 09_production_hardening, 10_metadata_gaps | completed |
| 13_autopsy_fixes | 12_column_semantic_search | completed |
| 14_multi_exchange | 12_column_semantic_search | completed |
| 15_code_quality | 14_multi_exchange | completed |
| 16_repo_scaffolding | 13_autopsy_fixes, 15_code_quality | completed |
| 17_routing_and_pipeline | 13_autopsy_fixes, 16_repo_scaffolding | completed |
| 18_yaml_schema_enrichment | 17_routing_and_pipeline | new |
| 19_embedding_enrichment | 18_yaml_schema_enrichment | new |
| 20_few_shot_expansion | 17_routing_and_pipeline | new |
| 21_metrics_and_filters | 18_yaml_schema_enrichment | new |
| 22_metadata_population | 18_yaml_schema_enrichment | new |

---

## DAG Visualization

```
Wave 1:  [01_foundation]    [07_dependency_fix]
              │
Wave 2:  [02_context_layer]
              │
              ├──────────────────────┐
Wave 3:  [03_agent_tools]    [06_metadata_enrichment]
              │                      │
Wave 4:  [04_agent_logic]           │
              │                      │
Wave 5:  [05_eval_hardening]        │
              │                      │
Wave 6:  [08_loop_and_performance]  │
              │                      │
Wave 7:  [09_production_hardening]  │
              │                      │
              ├──────────┬───────────┤
Wave 8:  [10_metadata]  │  [11_mcp]
              │          │
              ├──────────┘
Wave 10: [12_column_semantic_search]
              │
              ├──────────────────────┐
Wave 11: [13_autopsy_fixes]  [14_multi_exchange]
              │                      │
              │               [15_code_quality]
              │                      │
              ├──────────────────────┘
Wave 13: [16_repo_scaffolding]
              │
Wave 14: [17_routing_and_pipeline]
              │
              ├──────────────────────┐
Wave 15: [18_yaml_schema]    [20_few_shot_expansion]
              │
              ├──────────────────────┐
Wave 16: [19_embedding]      [21_metrics_and_filters]
              │
Wave 17: [22_metadata_population]
```

Note: Tracks within the same wave are independent and can run in parallel.
- Wave 15: Track 18 and Track 20 can run in parallel (both depend only on Track 17)
- Wave 16: Track 19 and Track 21 can run in parallel (both depend on Track 18)
