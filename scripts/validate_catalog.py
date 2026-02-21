#!/usr/bin/env python3
"""Validate YAML catalog files against Pydantic schema models.

Usage:
    # Validate all catalog files
    python scripts/validate_catalog.py --all

    # Validate specific files
    python scripts/validate_catalog.py catalog/kpi/markettrade.yaml catalog/data/theodata.yaml

    # Validate a dataset file
    python scripts/validate_catalog.py catalog/kpi/_dataset.yaml

Exit code 0 if all valid, 1 if any errors.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure project root is on sys.path for catalog.schema import
_PROJECT_ROOT = Path(__file__).parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import yaml
from pydantic import ValidationError

from catalog.schema import DatasetSchema, TableSchema

CATALOG_DIR = Path(__file__).parent.parent / "catalog"


def validate_table_yaml(path: Path) -> list[str]:
    """Validate a table YAML file against TableSchema."""
    errors = []
    with open(path) as f:
        data = yaml.safe_load(f)

    if "table" not in data:
        return [f"{path}: missing top-level 'table' key"]

    try:
        TableSchema(**data["table"])
    except ValidationError as e:
        for err in e.errors():
            loc = " -> ".join(str(x) for x in err["loc"])
            errors.append(f"{path}: {loc}: {err['msg']}")

    return errors


def validate_dataset_yaml(path: Path) -> list[str]:
    """Validate a _dataset.yaml file against DatasetSchema."""
    errors = []
    with open(path) as f:
        data = yaml.safe_load(f)

    if "dataset" not in data:
        return [f"{path}: missing top-level 'dataset' key"]

    try:
        DatasetSchema(**data["dataset"])
    except ValidationError as e:
        for err in e.errors():
            loc = " -> ".join(str(x) for x in err["loc"])
            errors.append(f"{path}: {loc}: {err['msg']}")

    return errors


def validate_file(path: Path) -> list[str]:
    """Validate a single YAML file (auto-detects table vs dataset)."""
    if path.name.startswith("_"):
        if path.name == "_dataset.yaml":
            return validate_dataset_yaml(path)
        # Skip other _ files (_routing.yaml, _exchanges.yaml)
        return []
    return validate_table_yaml(path)


def discover_all_files() -> list[Path]:
    """Find all table and dataset YAML files in the catalog."""
    files = []
    for layer in ["kpi", "data"]:
        layer_dir = CATALOG_DIR / layer
        if not layer_dir.exists():
            continue
        for yaml_file in sorted(layer_dir.glob("*.yaml")):
            # Skip non-table/dataset files
            if yaml_file.name in ("_routing.yaml", "_exchanges.yaml"):
                continue
            files.append(yaml_file)
    return files


def main():
    parser = argparse.ArgumentParser(
        description="Validate YAML catalog files against Pydantic schema."
    )
    parser.add_argument("files", nargs="*", type=Path, help="YAML files to validate")
    parser.add_argument("--all", action="store_true", help="Validate all catalog files")
    args = parser.parse_args()

    if args.all:
        files = discover_all_files()
    elif args.files:
        files = args.files
    else:
        parser.print_help()
        sys.exit(1)

    if not files:
        print("No files to validate.")
        sys.exit(0)

    all_errors = []
    for path in files:
        if not path.exists():
            all_errors.append(f"{path}: file not found")
            continue

        errors = validate_file(path)
        if errors:
            all_errors.extend(errors)
            print(f"FAIL  {path} ({len(errors)} error(s))")
            for err in errors:
                print(f"  {err}")
        else:
            col_count = ""
            if not path.name.startswith("_"):
                with open(path) as f:
                    data = yaml.safe_load(f)
                cols = data.get("table", {}).get("columns", [])
                col_count = f" ({len(cols)} columns)"
            print(f"OK    {path}{col_count}")

    print(f"\n{'=' * 60}")
    print(f"Files validated: {len(files)}")
    print(f"Errors: {len(all_errors)}")

    if all_errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
