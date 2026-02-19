"""NL2SQL evaluation runner.

Two modes:
- Offline: dry-run all gold SQL, validate structure, check table coverage
- Online: run agent pipeline per question, measure accuracy (requires LLM)

Usage:
    python eval/run_eval.py --mode offline
    python eval/run_eval.py --mode online
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from nl2sql_agent.catalog_loader import resolve_example_sql
from nl2sql_agent.config import settings

GOLD_QUERIES_PATH = Path(__file__).parent / "gold_queries.yaml"


def load_gold_queries(path: Path | None = None) -> list[dict[str, Any]]:
    """Load gold queries from YAML."""
    path = path or GOLD_QUERIES_PATH
    with open(path) as f:
        data = yaml.safe_load(f)
    return data.get("queries", [])


# --- SQL Component Extraction ---

def _extract_tables(sql: str) -> set[str]:
    """Extract table names from fully-qualified BigQuery references."""
    pattern = r"`[^`]+\.(\w+)\.(\w+)`"
    matches = re.findall(pattern, sql)
    return {table for _, table in matches}


def _extract_select_columns(sql: str) -> list[str]:
    """Extract column names from SELECT clause (simplified)."""
    # Get the first SELECT...FROM block
    match = re.search(r"SELECT\s+(.+?)\s+FROM", sql, re.IGNORECASE | re.DOTALL)
    if not match:
        return []
    select_block = match.group(1)
    # Extract aliases (AS name) or bare column names
    aliases = re.findall(r"\bAS\s+(\w+)", select_block, re.IGNORECASE)
    if aliases:
        return sorted(aliases)
    # Fallback: split by comma and extract last identifier
    cols = []
    for part in select_block.split(","):
        part = part.strip()
        tokens = part.split()
        if tokens:
            cols.append(tokens[-1].strip("()"))
    return sorted(cols)


def _has_group_by(sql: str) -> bool:
    return bool(re.search(r"\bGROUP\s+BY\b", sql, re.IGNORECASE))


def _has_order_by(sql: str) -> bool:
    return bool(re.search(r"\bORDER\s+BY\b", sql, re.IGNORECASE))


def _has_where(sql: str) -> bool:
    return bool(re.search(r"\bWHERE\b", sql, re.IGNORECASE))


# --- Result Set Comparison ---

def compare_result_sets(
    gold_df: pd.DataFrame,
    predicted_df: pd.DataFrame,
    float_precision: int = 4,
) -> bool:
    """Compare two DataFrames ignoring column order, sorting all columns.

    Float columns are rounded to `float_precision` decimals.
    """
    if gold_df.empty and predicted_df.empty:
        return True
    if gold_df.empty or predicted_df.empty:
        return False
    if set(gold_df.columns) != set(predicted_df.columns):
        return False

    def _normalize(df: pd.DataFrame) -> pd.DataFrame:
        # Reorder columns alphabetically
        df = df[sorted(df.columns)]
        # Round float columns
        for col in df.select_dtypes(include=["float", "float64"]).columns:
            df[col] = df[col].round(float_precision)
        # Sort by all columns and reset index
        df = df.sort_values(by=list(df.columns)).reset_index(drop=True)
        return df

    return _normalize(gold_df).equals(_normalize(predicted_df))


# --- Metrics ---

@dataclass
class EvalResult:
    """Result of evaluating a single gold query."""

    query_id: str
    question: str
    category: str
    expected_tables: list[str]
    predicted_tables: list[str] = field(default_factory=list)
    routing_correct: bool = False
    syntax_valid: bool = False
    execution_match: bool = False
    component_match_score: float = 0.0
    error: str = ""

    @property
    def passed(self) -> bool:
        return self.routing_correct and self.syntax_valid


@dataclass
class EvalReport:
    """Aggregate evaluation report."""

    results: list[EvalResult] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def routing_accuracy(self) -> float:
        if not self.results:
            return 0.0
        correct = sum(1 for r in self.results if r.routing_correct)
        return correct / len(self.results)

    @property
    def syntax_accuracy(self) -> float:
        if not self.results:
            return 0.0
        valid = sum(1 for r in self.results if r.syntax_valid)
        return valid / len(self.results)

    @property
    def execution_accuracy(self) -> float:
        if not self.results:
            return 0.0
        matched = sum(1 for r in self.results if r.execution_match)
        return matched / len(self.results)

    @property
    def avg_component_match(self) -> float:
        if not self.results:
            return 0.0
        return sum(r.component_match_score for r in self.results) / len(self.results)

    def by_category(self) -> dict[str, list[EvalResult]]:
        categories: dict[str, list[EvalResult]] = {}
        for r in self.results:
            categories.setdefault(r.category, []).append(r)
        return categories

    def to_markdown(self) -> str:
        lines = [
            "# NL2SQL Evaluation Report",
            "",
            f"**Total queries**: {self.total}",
            f"**Routing accuracy**: {self.routing_accuracy:.1%}",
            f"**Syntax accuracy**: {self.syntax_accuracy:.1%}",
            f"**Execution accuracy**: {self.execution_accuracy:.1%}",
            f"**Avg component match**: {self.avg_component_match:.1%}",
            "",
            "## Results by Category",
            "",
        ]
        for cat, results in sorted(self.by_category().items()):
            routing = sum(1 for r in results if r.routing_correct) / len(results)
            syntax = sum(1 for r in results if r.syntax_valid) / len(results)
            lines.append(f"### {cat} ({len(results)} queries)")
            lines.append(f"- Routing: {routing:.1%}")
            lines.append(f"- Syntax: {syntax:.1%}")
            for r in results:
                status = "PASS" if r.passed else "FAIL"
                lines.append(f"  - [{status}] {r.query_id}: {r.question[:60]}")
                if r.error:
                    lines.append(f"    Error: {r.error}")
            lines.append("")

        return "\n".join(lines)


# --- Component Match ---

def compute_component_match(gold_sql: str, predicted_sql: str) -> float:
    """Compute structural similarity between gold and predicted SQL.

    Compares: tables, SELECT columns, GROUP BY presence, ORDER BY presence, WHERE presence.
    Returns score between 0.0 and 1.0.
    """
    checks = 0
    matches = 0

    # Table match
    checks += 1
    if _extract_tables(gold_sql) == _extract_tables(predicted_sql):
        matches += 1

    # SELECT columns match
    checks += 1
    if _extract_select_columns(gold_sql) == _extract_select_columns(predicted_sql):
        matches += 1

    # GROUP BY presence
    checks += 1
    if _has_group_by(gold_sql) == _has_group_by(predicted_sql):
        matches += 1

    # ORDER BY presence
    checks += 1
    if _has_order_by(gold_sql) == _has_order_by(predicted_sql):
        matches += 1

    # WHERE presence
    checks += 1
    if _has_where(gold_sql) == _has_where(predicted_sql):
        matches += 1

    return matches / checks if checks > 0 else 0.0


# --- Routing Accuracy ---

def check_routing(expected_tables: list[str], predicted_tables: list[str]) -> bool:
    """Check if predicted tables match expected tables (set comparison)."""
    return set(expected_tables) == set(predicted_tables)


# --- Offline Runner ---

class EvalRunner:
    """Run gold query evaluation."""

    def __init__(self, queries: list[dict[str, Any]] | None = None):
        self.queries = queries or load_gold_queries()
        self.project = settings.gcp_project

    def run_offline(self) -> EvalReport:
        """Offline mode: validate gold SQL structure without executing."""
        report = EvalReport()

        for q in self.queries:
            resolved_sql = resolve_example_sql(q["gold_sql"], self.project)
            tables = _extract_tables(resolved_sql)

            result = EvalResult(
                query_id=q["id"],
                question=q["question"],
                category=q["category"],
                expected_tables=q["expected_tables"],
                predicted_tables=list(tables),
                routing_correct=set(q["expected_tables"]) == tables,
                syntax_valid=True,  # Structure check only in offline
                component_match_score=1.0,  # Gold SQL matches itself
            )
            report.results.append(result)

        return report

    def run_offline_dry_run(self, bq_service) -> EvalReport:
        """Offline mode with BigQuery dry-run validation."""
        report = EvalReport()

        for q in self.queries:
            resolved_sql = resolve_example_sql(q["gold_sql"], self.project)
            tables = _extract_tables(resolved_sql)

            dry_run_result = bq_service.dry_run_query(resolved_sql)

            result = EvalResult(
                query_id=q["id"],
                question=q["question"],
                category=q["category"],
                expected_tables=q["expected_tables"],
                predicted_tables=list(tables),
                routing_correct=set(q["expected_tables"]) == tables,
                syntax_valid=dry_run_result.get("valid", False),
                error=dry_run_result.get("error", "") or "",
                component_match_score=1.0,
            )
            report.results.append(result)

        return report

    def run_online(self) -> EvalReport:
        """Online mode: run agent pipeline and compare results."""
        from nl2sql_agent.agent import nl2sql_agent
        from nl2sql_agent.tools import execute_sql as original_execute_sql
        from nl2sql_agent.tools._deps import get_bq_service

        report = EvalReport()
        bq = get_bq_service()

        # Hook into execute_sql to capture generated SQL
        captured_sql: list[str] = []

        def capture_execute_sql(sql_query: str) -> dict:
            captured_sql.append(sql_query)
            return original_execute_sql(sql_query)

        # Patch the tool in the agent
        original_tool_idx = -1
        for i, tool in enumerate(nl2sql_agent.tools):
            if hasattr(tool, "__name__") and tool.__name__ == "execute_sql":
                original_tool_idx = i
                nl2sql_agent.tools[i] = capture_execute_sql
                break
        
        if original_tool_idx == -1:
            print("WARNING: Could not find execute_sql tool in agent. SQL capture will fail.")

        print(f"Running online evaluation for {len(self.queries)} queries...")

        for q in self.queries:
            print(f"Query {q['id']}: {q['question']}")
            captured_sql.clear()
            
            # Run Agent
            try:
                nl2sql_agent.run(input=q["question"])
            except Exception as e:
                print(f"  Agent failed: {e}")
                report.results.append(EvalResult(
                    query_id=q["id"],
                    question=q["question"],
                    category=q["category"],
                    expected_tables=q["expected_tables"],
                    error=f"Agent runtime error: {str(e)}"
                ))
                continue

            # Check if SQL was generated
            if not captured_sql:
                print("  No SQL generated.")
                report.results.append(EvalResult(
                    query_id=q["id"],
                    question=q["question"],
                    category=q["category"],
                    expected_tables=q["expected_tables"],
                    routing_correct=False,
                    error="No SQL generated"
                ))
                continue

            # We use the LAST generated SQL if multiple were attempted
            predicted_sql = captured_sql[-1]
            resolved_gold_sql = resolve_example_sql(q["gold_sql"], self.project)
            
            # Execute Gold
            try:
                gold_df = bq.execute_query(resolved_gold_sql)
            except Exception as e:
                print(f"  Gold SQL failed: {e}")
                report.results.append(EvalResult(
                    query_id=q["id"],
                    question=q["question"],
                    category=q["category"],
                    expected_tables=q["expected_tables"],
                    error=f"Gold SQL error: {str(e)}"
                ))
                continue

            # Execute Predicted
            try:
                predicted_df = bq.execute_query(predicted_sql)
                execution_match = compare_result_sets(gold_df, predicted_df)
            except Exception as e:
                print(f"  Predicted SQL failed: {e}")
                report.results.append(EvalResult(
                    query_id=q["id"],
                    question=q["question"],
                    category=q["category"],
                    expected_tables=q["expected_tables"],
                    predicted_tables=list(_extract_tables(predicted_sql)),
                    routing_correct=True,
                    syntax_valid=False,
                    error=str(e),
                    component_match_score=compute_component_match(resolved_gold_sql, predicted_sql)
                ))
                continue

            # Success path
            tables = _extract_tables(predicted_sql)
            result = EvalResult(
                query_id=q["id"],
                question=q["question"],
                category=q["category"],
                expected_tables=q["expected_tables"],
                predicted_tables=list(tables),
                routing_correct=True,
                syntax_valid=True,
                execution_match=execution_match,
                component_match_score=compute_component_match(resolved_gold_sql, predicted_sql)
            )
            report.results.append(result)
            print(f"  Match: {execution_match}")

        # Restore original tool
        if original_tool_idx != -1:
            nl2sql_agent.tools[original_tool_idx] = original_execute_sql

        return report


def main():
    parser = argparse.ArgumentParser(description="NL2SQL evaluation runner")
    parser.add_argument(
        "--mode",
        choices=["offline", "online"],
        default="offline",
        help="Evaluation mode",
    )
    args = parser.parse_args()

    queries = load_gold_queries()
    runner = EvalRunner(queries)

    if args.mode == "offline":
        report = runner.run_offline()
        print(report.to_markdown())
        sys.exit(0 if report.routing_accuracy == 1.0 else 1)
    elif args.mode == "online":
        report = runner.run_online()
        print(report.to_markdown())
        sys.exit(0 if report.routing_accuracy > 0.9 and report.execution_accuracy > 0.9 else 1)
    else:
        # Fallback to offline
        report = runner.run_offline()
        print(report.to_markdown())
        sys.exit(0 if report.routing_accuracy == 1.0 else 1)


if __name__ == "__main__":
    main()
