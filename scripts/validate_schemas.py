"""Compare YAML catalog columns against BQ schemas (offline or live).

Usage:
    # Against committed schema JSONs (no BQ credentials needed)
    python scripts/validate_schemas.py --offline

    # Against live BQ INFORMATION_SCHEMA (needs ADC credentials)
    python scripts/validate_schemas.py --live
"""

import argparse
import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).parent.parent
SCHEMA_DIR = ROOT / "schemas"
CATALOG_DIR = ROOT / "catalog"

# Tables that have both a catalog YAML and a BQ schema
TABLE_MAP = {
    "kpi": ["brokertrade", "clicktrade", "markettrade", "otoswing", "quotertrade"],
    "data": [
        "clicktrade",
        "markettrade",
        "swingdata",
        "quotertrade",
        "theodata",
        "marketdata",
        "marketdepth",
    ],
}


def load_bq_columns_offline(layer: str, table: str) -> set[str]:
    """Load column names from committed schema JSON."""
    path = SCHEMA_DIR / layer / f"{table}.json"
    if not path.exists():
        print(f"  SKIP {layer}/{table}: no schema JSON at {path}")
        return set()
    with open(path) as f:
        schema = json.load(f)
    return {col["name"] for col in schema}


def load_bq_columns_live(client, project: str, dataset: str, table: str) -> set[str]:
    """Load column names from live BQ INFORMATION_SCHEMA."""
    table_ref = f"{project}.{dataset}.{table}"
    bq_table = client.get_table(table_ref)
    return {field.name for field in bq_table.schema}


def load_yaml_columns(layer: str, table: str) -> set[str]:
    """Load column names from catalog YAML."""
    path = CATALOG_DIR / layer / f"{table}.yaml"
    if not path.exists():
        return set()
    with open(path) as f:
        data = yaml.safe_load(f)
    return {
        col["name"]
        for col in data.get("table", {}).get("columns", [])
        if isinstance(col, dict) and "name" in col
    }


def diff_table(
    layer: str,
    table: str,
    bq_cols: set[str],
    yaml_cols: set[str],
) -> tuple[set[str], set[str]]:
    """Return (hallucinated, missing) column sets."""
    hallucinated = yaml_cols - bq_cols  # in YAML but not in BQ
    missing = bq_cols - yaml_cols  # in BQ but not in YAML
    return hallucinated, missing


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare YAML catalog columns against BQ schemas"
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Query live BQ INFORMATION_SCHEMA (needs ADC credentials)",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Use committed schemas/*.json files (default)",
    )
    args = parser.parse_args()

    if args.live:
        from google.cloud import bigquery

        from nl2sql_agent.config import Settings

        s = Settings()
        client = bigquery.Client(project=s.gcp_project, location=s.bq_location)
        dataset_names = {"kpi": s.kpi_dataset, "data": s.data_dataset}
    elif not args.offline and not args.live:
        args.offline = True  # default

    has_errors = False
    total_hallucinated = 0
    total_missing = 0

    for layer, tables in TABLE_MAP.items():
        for table in tables:
            yaml_cols = load_yaml_columns(layer, table)
            if not yaml_cols:
                print(f"  SKIP {layer}/{table}: no catalog YAML")
                continue

            if args.live:
                bq_cols = load_bq_columns_live(
                    client, s.gcp_project, dataset_names[layer], table
                )
            else:
                bq_cols = load_bq_columns_offline(layer, table)

            if not bq_cols:
                continue

            hallucinated, missing = diff_table(layer, table, bq_cols, yaml_cols)

            if hallucinated:
                has_errors = True
                total_hallucinated += len(hallucinated)
                print(
                    f"  ERROR {layer}/{table}: "
                    f"{len(hallucinated)} YAML columns NOT in BQ: "
                    f"{sorted(hallucinated)}"
                )

            if missing:
                total_missing += len(missing)
                print(
                    f"  WARN  {layer}/{table}: "
                    f"{len(missing)} BQ columns not in YAML "
                    f"(first 10): {sorted(missing)[:10]}"
                )

            if not hallucinated and not missing:
                print(f"  OK    {layer}/{table}: {len(yaml_cols)} columns match")

    print()
    print(
        f"Summary: {total_hallucinated} hallucinated, {total_missing} missing (warnings)"
    )

    if has_errors:
        print("FAILED: hallucinated columns found in YAML catalog")
        sys.exit(1)
    else:
        print("PASSED: no hallucinated columns")
        sys.exit(0)


if __name__ == "__main__":
    main()
