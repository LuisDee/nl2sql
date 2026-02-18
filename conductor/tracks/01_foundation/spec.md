# Specification: Foundation (Phase A)

## Goal
Establish the foundational infrastructure for the NL2SQL agent by setting up the repository structure, configuring the development environment, creating the BigQuery development dataset with sample data, extracting initial schemas, and implementing the basic "Root Agent -> Sub-Agent" delegation skeleton.

## Requirements
- **Repo Scaffolding:** Create directory structure `agent/nl2sql`, `catalog/`, `examples/`, `embeddings/`, `setup/`, `eval/`. Initialize `pyproject.toml` with dependencies (`google-adk`, `google-cloud-bigquery`, `pyyaml`).
- **Dev Dataset:** Create SQL scripts to setup `dev_agent_test` dataset and populate `theodata`, `kpi_*`, and `quoter_quotertrade` tables with thin slices of production data (1 symbol, 30 days).
- **Schema Extraction:** Implement `setup/extract_schemas.py` to dump BigQuery schemas to JSON and generate YAML templates.
- **Agent Skeleton:** Implement `agent/root_agent.py` defining `mako_assistant` and `agent/nl2sql/agent.py` defining `nl2sql_agent`. Ensure delegation logic is in place.
- **Configuration:** Use `pydantic-settings` to manage configuration (`GCP_PROJECT`, `BQ_LOCATION`, `LITELLM_PROXY_URL`) loaded from `.env`.
- **Testing:** Setup `pytest` infrastructure and write unit tests for agent initialization and delegation.

## Success Criteria
- [ ] Directory structure created matching the plan.
- [ ] `pyproject.toml` installed and environment active.
- [ ] `dev_agent_test` dataset exists in BigQuery with populated tables.
- [ ] JSON schemas and YAML templates generated in `catalog/`.
- [ ] `adk web agent/` launches successfully.
- [ ] Root agent delegates "Show trades" requests to the sub-agent.
- [ ] Unit tests for agent classes pass.
