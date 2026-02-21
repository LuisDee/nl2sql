"""Load and validate YAML catalog files.

This module is used by:
- Tests (to validate YAML structure)
- The populate script (to load YAML into BQ embedding tables)
- Future: the agent's metadata_loader tool (Track 03)

Usage:
    from nl2sql_agent.catalog_loader import load_catalog, load_examples
"""

import functools
from pathlib import Path
from typing import Any

import yaml

from nl2sql_agent.logging_config import get_logger

logger = get_logger(__name__)

CATALOG_DIR = Path(__file__).parent.parent / "catalog"
EXAMPLES_DIR = Path(__file__).parent.parent / "examples"

REQUIRED_TABLE_KEYS = {
    "name",
    "dataset",
    "fqn",
    "layer",
    "description",
    "partition_field",
    "columns",
}
REQUIRED_COLUMN_KEYS = {"name", "type", "description"}
REQUIRED_EXAMPLE_KEYS = {"question", "sql", "tables_used", "dataset", "complexity"}
VALID_LAYERS = {"kpi", "data"}
VALID_DATASETS = {"{kpi_dataset}", "{data_dataset}"}
VALID_COMPLEXITIES = {"simple", "medium", "complex"}


@functools.lru_cache(maxsize=50)
def load_yaml(path: Path) -> dict[str, Any]:
    """Load a single YAML file (cached after first read).

    Path objects are hashable, so they work as lru_cache keys.

    Args:
        path: Path to the YAML file.

    Returns:
        Parsed YAML content as a dict.

    Raises:
        FileNotFoundError: If the file doesn't exist.
        yaml.YAMLError: If the YAML is malformed.
    """
    with open(path) as f:
        return yaml.safe_load(f)  # type: ignore[no-any-return]


def clear_yaml_cache() -> None:
    """Clear the load_yaml LRU cache (for test isolation or hot-reload)."""
    load_yaml.cache_clear()


def resolve_placeholders(
    text: str,
    *,
    project: str = "",
    kpi_dataset: str = "",
    data_dataset: str = "",
) -> str:
    """Resolve all catalog placeholders in a string.

    Handles {project}, {kpi_dataset}, and {data_dataset} placeholders.
    Only replaces placeholders for which a non-empty value is provided.
    """
    result = text
    if project:
        result = result.replace("{project}", project)
    if kpi_dataset:
        result = result.replace("{kpi_dataset}", kpi_dataset)
    if data_dataset:
        result = result.replace("{data_dataset}", data_dataset)
    return result


def resolve_fqn(
    table_data: dict[str, Any],
    project: str,
    kpi_dataset: str = "",
    data_dataset: str = "",
) -> str:
    """Resolve placeholders in a table's fqn field.

    Args:
        table_data: The 'table' dict from a table YAML (must have 'fqn' key).
        project: GCP project ID to substitute.
        kpi_dataset: KPI dataset name (optional).
        data_dataset: Data dataset name (optional).

    Returns:
        Fully-qualified table name with placeholders resolved.
    """
    return resolve_placeholders(
        table_data["fqn"],
        project=project,
        kpi_dataset=kpi_dataset,
        data_dataset=data_dataset,
    )


def resolve_example_sql(
    sql: str,
    project: str,
    kpi_dataset: str = "",
    data_dataset: str = "",
) -> str:
    """Resolve placeholders in example SQL.

    Args:
        sql: SQL string with placeholders.
        project: GCP project ID to substitute.
        kpi_dataset: KPI dataset name (optional).
        data_dataset: Data dataset name (optional).

    Returns:
        SQL string with all placeholders replaced.
    """
    return resolve_placeholders(
        sql,
        project=project,
        kpi_dataset=kpi_dataset,
        data_dataset=data_dataset,
    )


def validate_table_yaml(data: dict[str, Any], filepath: str = "") -> list[str]:
    """Validate a table YAML file against the required schema.

    Args:
        data: Parsed YAML content.
        filepath: Optional filepath for error messages.

    Returns:
        List of validation error strings. Empty list = valid.
    """
    errors: list[str] = []
    prefix = f"{filepath}: " if filepath else ""

    if "table" not in data:
        errors.append(f"{prefix}Missing top-level 'table' key")
        return errors

    table = data["table"]
    missing = REQUIRED_TABLE_KEYS - set(table.keys())
    if missing:
        errors.append(f"{prefix}Missing table keys: {missing}")

    if table.get("layer") not in VALID_LAYERS:
        errors.append(
            f"{prefix}Invalid layer: {table.get('layer')}. Must be one of {VALID_LAYERS}"
        )

    if table.get("dataset") not in VALID_DATASETS:
        errors.append(
            f"{prefix}Invalid dataset: {table.get('dataset')}. Must be one of {VALID_DATASETS}"
        )

    fqn = table.get("fqn", "")
    if "{project}" not in fqn:
        errors.append(f"{prefix}fqn must contain {{project}} placeholder, got: {fqn}")

    columns = table.get("columns", [])
    if not isinstance(columns, list):
        errors.append(f"{prefix}columns must be a list")
    else:
        for i, col in enumerate(columns):
            col_missing = REQUIRED_COLUMN_KEYS - set(col.keys())
            if col_missing:
                errors.append(
                    f"{prefix}Column {i} ({col.get('name', '?')}): missing keys {col_missing}"
                )

    return errors


def validate_dataset_yaml(data: dict[str, Any], filepath: str = "") -> list[str]:
    """Validate a dataset YAML file."""
    errors: list[str] = []
    prefix = f"{filepath}: " if filepath else ""

    if "dataset" not in data:
        errors.append(f"{prefix}Missing top-level 'dataset' key")
        return errors

    ds = data["dataset"]
    if "name" not in ds:
        errors.append(f"{prefix}Missing dataset.name")
    if "tables" not in ds:
        errors.append(f"{prefix}Missing dataset.tables")

    return errors


def validate_examples_yaml(data: dict[str, Any], filepath: str = "") -> list[str]:
    """Validate an examples YAML file."""
    errors: list[str] = []
    prefix = f"{filepath}: " if filepath else ""

    if "examples" not in data:
        errors.append(f"{prefix}Missing top-level 'examples' key")
        return errors

    examples = data["examples"]
    if not isinstance(examples, list):
        errors.append(f"{prefix}examples must be a list")
        return errors

    for i, ex in enumerate(examples):
        missing = REQUIRED_EXAMPLE_KEYS - set(ex.keys())
        if missing:
            errors.append(f"{prefix}Example {i}: missing keys {missing}")

        if ex.get("complexity") not in VALID_COMPLEXITIES:
            errors.append(
                f"{prefix}Example {i}: invalid complexity '{ex.get('complexity')}'"
            )

        if ex.get("dataset") not in VALID_DATASETS:
            errors.append(f"{prefix}Example {i}: invalid dataset '{ex.get('dataset')}'")

        sql = ex.get("sql", "")
        if "{project}" not in sql:
            errors.append(
                f"{prefix}Example {i}: SQL must use {{project}} placeholder for fully-qualified table names"
            )

    return errors


def load_all_table_yamls() -> list[dict[str, Any]]:
    """Load all table YAML files from catalog/kpi/ and catalog/data/.

    Returns:
        List of parsed table YAML dicts.
    """
    tables = []
    for subdir in ["kpi", "data"]:
        dir_path = CATALOG_DIR / subdir
        if not dir_path.exists():
            continue
        for yaml_file in sorted(dir_path.glob("*.yaml")):
            if yaml_file.name.startswith("_"):
                continue  # Skip _dataset.yaml
            data = load_yaml(yaml_file)
            if "table" in data:
                tables.append(data)
    return tables


@functools.lru_cache(maxsize=1)
def load_exchange_registry() -> dict[str, Any]:
    """Load the exchange registry from catalog/_exchanges.yaml (cached).

    Returns:
        Parsed YAML dict with 'default_exchange' and 'exchanges' keys.

    Raises:
        FileNotFoundError: If _exchanges.yaml doesn't exist.
    """
    path = CATALOG_DIR / "_exchanges.yaml"
    return load_yaml(path)


def clear_exchange_cache() -> None:
    """Clear the exchange registry LRU cache (for test isolation)."""
    load_exchange_registry.cache_clear()


@functools.lru_cache(maxsize=1)
def load_routing_rules() -> dict[str, Any]:
    """Load routing rules from all YAML sources (cached).

    Combines:
    - catalog/_routing.yaml (cross-cutting routing descriptions)
    - catalog/kpi/_dataset.yaml routing section (KPI pattern→table)
    - catalog/data/_dataset.yaml routing section (data pattern→table)

    Returns:
        Dict with 'cross_cutting', 'kpi_routing', 'data_routing' keys.
    """
    # Cross-cutting routing descriptions
    routing_path = CATALOG_DIR / "_routing.yaml"
    cross_cutting: dict[str, Any] = {}
    if routing_path.exists():
        routing_data = load_yaml(routing_path)
        cross_cutting = routing_data.get("routing_descriptions", {})

    # KPI routing rules
    kpi_ds_path = CATALOG_DIR / "kpi" / "_dataset.yaml"
    kpi_routing: list[dict[str, Any]] = []
    if kpi_ds_path.exists():
        kpi_data = load_yaml(kpi_ds_path)
        kpi_routing = kpi_data.get("dataset", {}).get("routing", [])

    # Data routing rules
    data_ds_path = CATALOG_DIR / "data" / "_dataset.yaml"
    data_routing: list[dict[str, Any]] = []
    if data_ds_path.exists():
        data_data = load_yaml(data_ds_path)
        data_routing = data_data.get("dataset", {}).get("routing", [])

    return {
        "cross_cutting": cross_cutting,
        "kpi_routing": kpi_routing,
        "data_routing": data_routing,
    }


def clear_routing_cache() -> None:
    """Clear the routing rules LRU cache (for test isolation)."""
    load_routing_rules.cache_clear()


def load_all_examples() -> list[dict[str, Any]]:
    """Load all example YAML files from examples/.

    Returns:
        Flat list of all example dicts across all files.
    """
    all_examples = []
    for yaml_file in sorted(EXAMPLES_DIR.glob("*.yaml")):
        data = load_yaml(yaml_file)
        if "examples" in data and isinstance(data["examples"], list):
            all_examples.extend(data["examples"])
    return all_examples
