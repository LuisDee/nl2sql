#!/usr/bin/env bash
# Start the NL2SQL agent locally via ADK.
#
# Usage:
#   scripts/start_local.sh        # Web UI on :8000 (default)
#   scripts/start_local.sh -t     # Terminal / interactive mode
#
set -euo pipefail

# --- Colors -----------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BOLD='\033[1m'
NC='\033[0m' # No Color

info()  { printf "${GREEN}[ok]${NC}  %s\n" "$*"; }
warn()  { printf "${YELLOW}[warn]${NC} %s\n" "$*"; }
fail()  { printf "${RED}[error]${NC} %s\n" "$*"; exit 1; }

# --- Parse flags ------------------------------------------------------
MODE="web"
while getopts "t" opt; do
  case $opt in
    t) MODE="terminal" ;;
    *) echo "Usage: $0 [-t]"; exit 1 ;;
  esac
done

# --- Resolve repo root (works from any CWD) ---------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# --- Prerequisite checks ---------------------------------------------

# Python 3.11+
if ! command -v python3 &>/dev/null; then
  fail "python3 not found. Install Python 3.11+."
fi
PY_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
PY_MAJOR=$(echo "$PY_VERSION" | cut -d. -f1)
PY_MINOR=$(echo "$PY_VERSION" | cut -d. -f2)
if [ "$PY_MAJOR" -lt 3 ] || { [ "$PY_MAJOR" -eq 3 ] && [ "$PY_MINOR" -lt 11 ]; }; then
  fail "Python 3.11+ required (found $PY_VERSION)."
fi
info "Python $PY_VERSION"

# gcloud ADC
if ! gcloud auth application-default print-access-token &>/dev/null 2>&1; then
  fail "No Application Default Credentials. Run: gcloud auth application-default login"
fi
info "GCP Application Default Credentials"

# adk installed
if ! command -v adk &>/dev/null; then
  fail "adk not found. Install: pip install google-adk"
fi
info "ADK CLI ($(adk --version 2>/dev/null || echo 'version unknown'))"

# .env exists
ENV_FILE="$REPO_ROOT/nl2sql_agent/.env"
if [ ! -f "$ENV_FILE" ]; then
  fail ".env not found at $ENV_FILE\n       Copy the template: cp nl2sql_agent/.env.example nl2sql_agent/.env"
fi
info ".env file"

# LiteLLM reachable
LITELLM_BASE=$(grep -E '^LITELLM_API_BASE=' "$ENV_FILE" | head -1 | cut -d= -f2-)
# Strip Docker-internal hostname for local checks
LITELLM_CHECK_URL="${LITELLM_BASE/host.docker.internal/localhost}"
if curl -sf "${LITELLM_CHECK_URL}/health" &>/dev/null; then
  info "LiteLLM proxy reachable at $LITELLM_BASE"
else
  warn "LiteLLM proxy not reachable at $LITELLM_BASE"
  warn "Start it with: scripts/start_litellm.sh"
fi

# --- Launch -----------------------------------------------------------
echo ""
if [ "$MODE" = "terminal" ]; then
  printf "${BOLD}Starting NL2SQL agent (terminal mode)...${NC}\n"
  cd "$REPO_ROOT"
  exec adk run nl2sql_agent
else
  printf "${BOLD}Starting NL2SQL agent (web UI on http://localhost:8000)...${NC}\n"
  cd "$REPO_ROOT"
  exec adk web nl2sql_agent
fi
