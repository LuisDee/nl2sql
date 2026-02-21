"""Exchange resolution tool for multi-exchange routing.

Resolves an exchange name, alias, or trading symbol to the correct
BigQuery dataset pair (kpi + data). Three-tier resolution:

1. Alias lookup — O(1) dict from _exchanges.yaml (no BQ needed)
2. Symbol lookup — BQ query against symbol_exchange_map table
3. Default fallback — returns the default exchange from registry
"""

from nl2sql_agent.catalog_loader import load_exchange_registry
from nl2sql_agent.config import settings
from nl2sql_agent.logging_config import get_logger
from nl2sql_agent.types import ExchangeMultipleResult, ExchangeResolvedResult

logger = get_logger(__name__)


def _build_alias_map() -> dict[str, str]:
    """Build a flat alias→canonical_name lookup from the exchange registry."""
    registry = load_exchange_registry()
    alias_map: dict[str, str] = {}
    for name, info in registry["exchanges"].items():
        for alias in info["aliases"]:
            alias_map[alias.lower()] = name
    return alias_map


def _make_result(exchange: str, status: str = "resolved") -> dict:
    """Build a standard result dict for a resolved exchange."""
    registry = load_exchange_registry()
    info = registry["exchanges"][exchange]
    return {
        "status": status,
        "exchange": exchange,
        "kpi_dataset": info["kpi_dataset"],
        "data_dataset": info["data_dataset"],
    }


def _default_result() -> dict:
    """Return the default exchange result."""
    registry = load_exchange_registry()
    return _make_result(registry["default_exchange"], status="default")


def _symbol_lookup(identifier: str) -> dict | None:
    """Tier 2: Look up a symbol in BQ symbol_exchange_map.

    Returns None if BQ is unavailable or symbol not found.
    Returns a result dict with status 'resolved' (single match)
    or 'multiple' (ambiguous — multiple exchanges).
    """
    from nl2sql_agent.tools._deps import get_bq_service

    try:
        bq = get_bq_service()
    except RuntimeError:
        return None

    sql = f"""
    SELECT DISTINCT exchange, portfolio
    FROM `{settings.gcp_project}.{settings.metadata_dataset}.symbol_exchange_map`
    WHERE UPPER(symbol) = UPPER(@symbol)
    ORDER BY exchange
    """
    try:
        rows = bq.query_with_params(
            sql, [{"name": "symbol", "type": "STRING", "value": identifier}]
        )
    except Exception as e:
        logger.warning("symbol_lookup_failed", error=str(e))
        return None

    if not rows:
        return None

    exchanges = list({r["exchange"] for r in rows})
    if len(exchanges) == 1:
        exchange = exchanges[0]
        registry = load_exchange_registry()
        if exchange in registry["exchanges"]:
            return _make_result(exchange)
        return None

    # Multiple exchanges — return all matches for LLM disambiguation
    registry = load_exchange_registry()
    matches = []
    for exchange in sorted(exchanges):
        if exchange not in registry["exchanges"]:
            continue
        info = registry["exchanges"][exchange]
        portfolios = [r["portfolio"] for r in rows if r["exchange"] == exchange]
        matches.append(
            {
                "exchange": exchange,
                "kpi_dataset": info["kpi_dataset"],
                "data_dataset": info["data_dataset"],
                "portfolios": portfolios,
            }
        )
    return {
        "status": "multiple",
        "message": (
            f"Symbol '{identifier}' found on {len(matches)} exchanges. "
            "Please clarify which exchange."
        ),
        "matches": matches,
    }


def resolve_exchange(
    exchange_or_symbol: str,
) -> ExchangeResolvedResult | ExchangeMultipleResult:
    """Resolve an exchange name, alias, or trading symbol to BQ datasets.

    Use this tool when the user's question mentions a specific exchange
    (e.g., "bovespa", "ASX", "eurex") or a trading symbol (e.g., "VALE3",
    "ABBS") that may indicate which exchange to query. Do NOT call this
    tool when the question doesn't reference any exchange or symbol.

    Resolution order:
    1. Alias match (instant, no BQ call) — checks exchange names and
       aliases like "bovespa"→brazil, "tsx"→canada
    2. Symbol lookup (BQ query) — checks symbol_exchange_map table
    3. Default fallback — returns the default exchange (OMX)

    After getting the result, use the returned kpi_dataset and data_dataset
    in fully-qualified table names for all subsequent SQL queries.

    Args:
        exchange_or_symbol: An exchange name ("brazil"), alias ("bovespa",
            "b3"), or trading symbol ("VALE3", "ABBS"). Case-insensitive.

    Returns:
        Dict with:
        - status: "resolved" (single match), "multiple" (ambiguous symbol),
          or "default" (fallback)
        - exchange: canonical exchange name
        - kpi_dataset: BQ dataset name for KPI tables
        - data_dataset: BQ dataset name for data tables
        - matches: (only if status="multiple") list of exchange options
    """
    identifier = exchange_or_symbol.strip()
    logger.info("resolve_exchange_start", identifier=identifier)

    # Tier 1: Alias lookup (O(1))
    alias_map = _build_alias_map()
    canonical = alias_map.get(identifier.lower())
    if canonical:
        result = _make_result(canonical)
        logger.info(
            "resolve_exchange_alias_hit", identifier=identifier, exchange=canonical
        )
        return result

    # Tier 2: Symbol lookup via BQ
    symbol_result = _symbol_lookup(identifier)
    if symbol_result:
        logger.info(
            "resolve_exchange_symbol_hit",
            identifier=identifier,
            status=symbol_result["status"],
        )
        return symbol_result

    # Tier 3: Default fallback
    result = _default_result()
    logger.info(
        "resolve_exchange_default", identifier=identifier, exchange=result["exchange"]
    )
    return result
