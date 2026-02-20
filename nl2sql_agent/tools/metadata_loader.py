"""YAML metadata loading tool for the NL2SQL agent.

Loads rich table descriptions, column metadata, synonyms, and business rules
from the YAML catalog files created in Track 02. This gives the LLM detailed
context about what each column means and how to write correct SQL.

Depends on: catalog_loader module from Track 02.
"""

import yaml

from nl2sql_agent.catalog_loader import load_yaml, CATALOG_DIR
from nl2sql_agent.config import settings
from nl2sql_agent.logging_config import get_logger

logger = get_logger(__name__)

# Map of unique table names to their YAML file paths (relative to CATALOG_DIR).
# For tables that exist in BOTH datasets (markettrade, quotertrade, clicktrade),
# they are NOT in this map — use _dataset_to_layer() with dataset name instead.
_TABLE_YAML_MAP: dict[str, str] = {
    # KPI-only tables
    "brokertrade": "kpi/brokertrade.yaml",
    "otoswing": "kpi/otoswing.yaml",
    # Data-only tables
    "theodata": "data/theodata.yaml",
    "swingdata": "data/swingdata.yaml",
    "marketdata": "data/marketdata.yaml",
    "marketdepth": "data/marketdepth.yaml",
}


def _dataset_to_layer(dataset_name: str) -> str | None:
    """Map a resolved dataset name to catalog layer (kpi or data).

    Uses settings to support any exchange — the mapping is driven by
    the KPI_DATASET and DATA_DATASET env vars, not hardcoded names.
    """
    if dataset_name == settings.kpi_dataset:
        return "kpi"
    if dataset_name == settings.data_dataset:
        return "data"
    return None


def _resolve_yaml_path(table_name: str, dataset_name: str = "") -> str | None:
    """Resolve a table name + optional dataset to a YAML file path.

    Uses dataset_to_layer() for dynamic resolution — works with any
    exchange dataset name (OMX, Brazil, ICE, etc.) as long as the
    KPI_DATASET/DATA_DATASET env vars match.

    Tries dataset+table first (most specific), then table alone.
    Returns None if no mapping found.
    """
    # Try dataset-based resolution first (most specific)
    if dataset_name:
        layer = _dataset_to_layer(dataset_name)
        if layer:
            return f"{layer}/{table_name}.yaml"

    # Try direct table name (unique tables only)
    if table_name in _TABLE_YAML_MAP:
        return _TABLE_YAML_MAP[table_name]

    # Try case-insensitive match
    for known_name, path in _TABLE_YAML_MAP.items():
        if known_name.lower() == table_name.lower():
            return path

    return None


def load_yaml_metadata(table_name: str, dataset_name: str) -> dict:
    """Load the YAML metadata catalog for a specific BigQuery table.

    Use this tool AFTER vector_search_columns to get detailed column
    descriptions, synonyms, business rules, and data types for the
    tables identified as relevant. This metadata is essential for
    generating correct SQL — it tells you exact column names, what
    they mean, what values they contain, and how calculations work.

    For KPI tables, the response also includes the KPI dataset context
    with shared column definitions and routing rules.

    Args:
        table_name: The table to load metadata for. Examples:
            'markettrade', 'theodata', 'quotertrade', 'brokertrade'.
        dataset_name: Dataset name to disambiguate tables that
            exist in both KPI and data datasets. Pass empty string
            if unknown.

    Returns:
        Dict with 'status' and either 'metadata' (the full YAML content
        as a string) or 'error_message' if the table was not found.
    """
    logger.info(
        "load_yaml_metadata_start",
        table_name=table_name,
        dataset_name=dataset_name,
    )

    yaml_path = _resolve_yaml_path(table_name, dataset_name)
    if yaml_path is None:
        logger.warning("load_yaml_metadata_not_found", table_name=table_name)
        return {
            "status": "error",
            "error_message": (
                f"No metadata found for table '{table_name}'"
                + (f" in dataset '{dataset_name}'" if dataset_name else "")
                + f". Known tables: {sorted(_TABLE_YAML_MAP.keys())}"
            ),
        }

    full_path = CATALOG_DIR / yaml_path
    if not full_path.exists():
        logger.error("load_yaml_metadata_file_missing", path=str(full_path))
        return {
            "status": "error",
            "error_message": f"YAML file not found at {full_path}. Was Track 02 completed?",
        }

    try:
        content = load_yaml(full_path)
    except Exception as e:
        logger.error("load_yaml_metadata_parse_error", path=str(full_path), error=str(e))
        return {"status": "error", "error_message": f"Failed to parse YAML: {e}"}

    # If this is a KPI table, also load the KPI dataset context
    if "kpi/" in yaml_path:
        dataset_yaml_path = CATALOG_DIR / "kpi" / "_dataset.yaml"
        if dataset_yaml_path.exists():
            try:
                dataset_context = load_yaml(dataset_yaml_path)
                content["_kpi_dataset_context"] = dataset_context
            except Exception:
                pass  # Non-fatal — table metadata is still useful without dataset context

    # If this is a data table, also load the data dataset context
    if "data/" in yaml_path:
        dataset_yaml_path = CATALOG_DIR / "data" / "_dataset.yaml"
        if dataset_yaml_path.exists():
            try:
                dataset_context = load_yaml(dataset_yaml_path)
                content["_data_dataset_context"] = dataset_context
            except Exception:
                pass

    # Convert to YAML string for the LLM (more readable than nested dict repr)
    metadata_str = yaml.dump(content, default_flow_style=False, sort_keys=False)

    logger.info(
        "load_yaml_metadata_complete",
        table_name=table_name,
        yaml_path=yaml_path,
        content_length=len(metadata_str),
    )

    return {
        "status": "success",
        "table_name": table_name,
        "dataset_name": dataset_name or content.get("table", {}).get("dataset", "unknown"),
        "metadata": metadata_str,
    }
