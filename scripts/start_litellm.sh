#!/usr/bin/env bash
# Start the local LiteLLM proxy (dev only — prod uses hosted proxy).
#
# Reads secrets from `pass` (GPG-encrypted password store) and starts
# litellm with the config at ~/.config/litellm/config.yaml.
#
# Prerequisites:
#   - pass (password-store) installed and initialised
#   - Secrets stored: api/claude, api/litellm-master
#   - litellm installed: pip install litellm
#   - Config at ~/.config/litellm/config.yaml
#
set -euo pipefail

# --- Colors -----------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BOLD='\033[1m'
NC='\033[0m'

info()  { printf "${GREEN}[ok]${NC}  %s\n" "$*"; }
warn()  { printf "${YELLOW}[warn]${NC} %s\n" "$*"; }
fail()  { printf "${RED}[error]${NC} %s\n" "$*"; exit 1; }

# --- Prerequisite checks ---------------------------------------------

if ! command -v pass &>/dev/null; then
  fail "pass not found. Install: brew install pass"
fi
info "pass (password-store)"

if ! command -v litellm &>/dev/null; then
  fail "litellm not found. Install: pip install litellm"
fi
info "litellm CLI"

CONFIG_PATH="$HOME/.config/litellm/config.yaml"
if [ ! -f "$CONFIG_PATH" ]; then
  fail "LiteLLM config not found at $CONFIG_PATH"
fi
info "Config: $CONFIG_PATH"

# --- Read secrets -----------------------------------------------------
printf "${BOLD}Reading secrets from pass...${NC}\n"

ANTHROPIC_API_KEY=$(pass show api/claude 2>/dev/null) \
  || fail "Could not read api/claude from pass"
info "api/claude"

LITELLM_MASTER_KEY=$(pass show api/litellm-master 2>/dev/null) \
  || fail "Could not read api/litellm-master from pass"
info "api/litellm-master"

# --- Start proxy ------------------------------------------------------
echo ""
printf "${BOLD}Starting LiteLLM proxy on http://localhost:4000 ...${NC}\n"
echo "  Models: claude-sonnet (Sonnet 4.5), claude-opus (Opus 4.6), claude-haiku (Haiku 4.5)"
echo "  Config: $CONFIG_PATH"
echo ""

export ANTHROPIC_API_KEY
export LITELLM_MASTER_KEY

exec litellm --config "$CONFIG_PATH"
