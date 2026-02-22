"""Extract BigQuery schemas to JSON ground truth files.

Reads project/dataset config from pydantic-settings (.env) and queries
INFORMATION_SCHEMA to extract column metadata for all tables.

Usage:
    python setup/extract_schemas.py

Output:
    schemas/kpi/{brokertrade,clicktrade,markettrade,otoswing,quotertrade}.json
    schemas/data/{clicktrade,markettrade,swingdata,quotertrade,theodata,marketdata,marketdepth}.json

Each JSON file contains an array of objects with: name, type, mode, description.
"""

import json
from pathlib import Path

from google.cloud import bigquery

from nl2sql_agent.config import Settings

# --- Configuration (from pydantic-settings / .env) ---
_settings = Settings()
PROJECT = _settings.gcp_project

DATASETS = {
    _settings.kpi_dataset: [
        "brokertrade",
        "clicktrade",
        "markettrade",
        "otoswing",
        "quotertrade",
    ],
    _settings.data_dataset: [
        "clicktrade",
        "markettrade",
        "swingdata",
        "quotertrade",
        "theodata",
        "marketdata",
        "marketdepth",
    ],
}

OUTPUT_DIR = Path("schemas")


def extract_schema(
    client: bigquery.Client, dataset: str, table_name: str
) -> list[dict]:
    """Extract schema from a BigQuery table.

    Args:
        client: BigQuery client instance.
        dataset: BigQuery dataset name.
        table_name: Name of the table.

    Returns:
        List of column dicts with keys: name, type, mode, description.
    """
    table_ref = f"{PROJECT}.{dataset}.{table_name}"
    table = client.get_table(table_ref)

    schema = [
        {
            "name": field.name,
            "type": field.field_type,
            "mode": field.mode,
            "description": field.description or "",
        }
        for field in table.schema
    ]

    return schema


def main() -> None:
    client = bigquery.Client(project=PROJECT, location=_settings.bq_location)

    for dataset, tables in DATASETS.items():
        # Create output subdirectory: schemas/kpi/ or schemas/data/
        short_name = "kpi" if "kpi" in dataset else "data"
        output_subdir = OUTPUT_DIR / short_name
        output_subdir.mkdir(parents=True, exist_ok=True)

        for table_name in tables:
            schema = extract_schema(client, dataset, table_name)

            output_path = output_subdir / f"{table_name}.json"
            with open(output_path, "w") as f:
                json.dump(schema, f, indent=2)

            print(f"✓ {dataset}.{table_name}: {len(schema)} columns → {output_path}")


if __name__ == "__main__":
    main()
