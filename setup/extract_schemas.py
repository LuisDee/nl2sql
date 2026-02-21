"""Extract BigQuery schemas from nl2sql_omx_kpi and nl2sql_omx_data tables.

Usage:
    python setup/extract_schemas.py

Output:
    schemas/kpi/brokertrade.json
    schemas/kpi/clicktrade.json
    schemas/kpi/markettrade.json
    schemas/kpi/otoswing.json
    schemas/kpi/quotertrade.json
    schemas/data/brokertrade.json
    schemas/data/clicktrade.json
    schemas/data/markettrade.json
    schemas/data/swingdata.json
    schemas/data/quotertrade.json
    schemas/data/theodata.json
    schemas/data/marketdata.json
    schemas/data/marketdepth.json

Each JSON file contains an array of objects with: name, type, mode, description.
"""

import json
from pathlib import Path

from google.cloud import bigquery

# --- Configuration ---
PROJECT = "cloud-data-n-base-d4b3"

DATASETS = {
    "nl2sql_omx_kpi": [
        "brokertrade",
        "clicktrade",
        "markettrade",
        "otoswing",
        "quotertrade",
    ],
    "nl2sql_omx_data": [
        "brokertrade",
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
    client = bigquery.Client(project=PROJECT, location="europe-west2")

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
