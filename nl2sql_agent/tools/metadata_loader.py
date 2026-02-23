"""YAML metadata loading tool for the NL2SQL agent.

Loads rich table descriptions, column metadata, synonyms, and business rules
from the YAML catalog files created in Track 02. This gives the LLM detailed
context about what each column means and how to write correct SQL.

Depends on: catalog_loader module from Track 02.
"""

import yaml

from nl2sql_agent.catalog_loader import CATALOG_DIR, load_yaml
from nl2sql_agent.config import settings
from nl2sql_agent.logging_config import get_logger
from nl2sql_agent.types import ErrorResult, MetadataSuccessResult

logger = get_logger(__name__)


def _discover_table_yaml_map() -> dict[str, str]:
    """Scan all catalog subdirectories and build table -> path map.

    Discovers all table YAML files dynamically so new tables don't
    require manual map updates. Skips _-prefixed files (e.g. _dataset.yaml).

    Scans catalog/kpi/, catalog/data/, and all catalog/*_data/ market
    directories. For tables in multiple directories, both are stored
    with prefixed keys (kpi/markettrade, data/markettrade, arb_data/markettrade).
    For the plain name key, kpi/ and data/ (OMX) take priority over market dirs.
    """
    from nl2sql_agent.catalog_loader import _catalog_subdirs

    primary_dirs = {"kpi", "data"}
    table_map: dict[str, str] = {}
    for subdir in _catalog_subdirs():
        subdir_path = CATALOG_DIR / subdir
        if not subdir_path.exists():
            continue
        for yaml_file in sorted(subdir_path.glob("*.yaml")):
            if yaml_file.name.startswith("_"):
                continue
            table_name = yaml_file.stem
            rel_path = f"{subdir}/{yaml_file.name}"
            # Store prefixed version for disambiguation
            table_map[f"{subdir}/{table_name}"] = rel_path
            # Plain name: primary dirs (kpi/data) always win over market dirs
            if table_name not in table_map or subdir in primary_dirs:
                table_map[table_name] = rel_path
    return table_map


_TABLE_YAML_MAP = _discover_table_yaml_map()


def _dataset_to_catalog_dir(dataset_name: str) -> str | None:
    """Map a resolved dataset name to catalog directory.

    For default exchange: nl2sql_omx_kpi -> kpi, nl2sql_omx_data -> data.
    For other markets: nl2sql_brazil_data -> brazil_data (if dir exists).
    Falls back to suffix-based heuristic.
    """
    # Quick check against default settings
    if dataset_name == settings.kpi_dataset:
        return "kpi"
    if dataset_name == settings.data_dataset:
        return "data"

    # Check if a market-specific catalog directory exists
    # Strip dataset_prefix to get the market dir name
    prefix = settings.dataset_prefix
    if prefix and dataset_name.startswith(prefix):
        market_dir = dataset_name[len(prefix) :]
        if (CATALOG_DIR / market_dir).exists():
            return market_dir

    # Heuristic fallback: suffix-based
    if dataset_name.endswith("_kpi"):
        return "kpi"
    if dataset_name.endswith("_data"):
        # Check if market dir exists (e.g. brazil_data)
        suffix = dataset_name.split(prefix)[-1] if prefix else dataset_name
        if (CATALOG_DIR / suffix).exists():
            return suffix
        return "data"

    return None


def _resolve_yaml_path(table_name: str, dataset_name: str = "") -> str | None:
    """Resolve a table name + optional dataset to a YAML file path.

    Uses _dataset_to_catalog_dir() for dynamic resolution — works with
    any exchange dataset name (OMX, Brazil, ICE, etc.).

    Tries dataset+table first (most specific), then table alone.
    Returns None if no mapping found.
    """
    # Try dataset-based resolution first (most specific)
    if dataset_name:
        catalog_dir = _dataset_to_catalog_dir(dataset_name)
        if catalog_dir:
            candidate = f"{catalog_dir}/{table_name}.yaml"
            if (CATALOG_DIR / candidate).exists():
                return candidate

    # Try prefixed lookup (market/table) from the map
    if dataset_name:
        catalog_dir = _dataset_to_catalog_dir(dataset_name)
        if catalog_dir:
            prefixed = f"{catalog_dir}/{table_name}"
            if prefixed in _TABLE_YAML_MAP:
                return _TABLE_YAML_MAP[prefixed]

    # Try direct table name (unique tables only)
    if table_name in _TABLE_YAML_MAP:
        return _TABLE_YAML_MAP[table_name]

    # Try case-insensitive match
    for known_name, path in _TABLE_YAML_MAP.items():
        if known_name.lower() == table_name.lower():
            return path

    return None


def load_yaml_metadata(
    table_name: str, dataset_name: str
) -> MetadataSuccessResult | ErrorResult:
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
        logger.error(
            "load_yaml_metadata_parse_error", path=str(full_path), error=str(e)
        )
        return {"status": "error", "error_message": f"Failed to parse YAML: {e}"}

    # Load dataset context from the same directory as the table YAML
    yaml_dir = yaml_path.split("/")[0]  # e.g. "kpi", "data", "brazil_data"
    dataset_yaml_path = CATALOG_DIR / yaml_dir / "_dataset.yaml"
    if dataset_yaml_path.exists():
        try:
            dataset_context = load_yaml(dataset_yaml_path)
            content["_dataset_context"] = dataset_context
        except Exception:  # noqa: S110
            pass  # Non-fatal — table metadata is still useful without dataset context

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
        "dataset_name": dataset_name
        or content.get("table", {}).get("dataset", "unknown"),
        "metadata": metadata_str,
    }
