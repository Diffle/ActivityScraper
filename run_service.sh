#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

if [[ ! -x ".venv/bin/python" ]]; then
  echo "Error: missing virtual environment. Run install_service.sh first." >&2
  exit 1
fi

required_vars=(TELEGRAM_BOT_TOKEN TELEGRAM_CHAT_ID)
for var_name in "${required_vars[@]}"; do
  if [[ -z "${!var_name:-}" ]]; then
    echo "Error: missing required env var: ${var_name}" >&2
    exit 1
  fi
done

wallet_args=()
if [[ -n "${POLYMARKET_WALLETS:-}" ]]; then
  wallet_args+=(--wallets "$POLYMARKET_WALLETS")
elif [[ -n "${POLYMARKET_WALLET:-}" ]]; then
  wallet_args+=(--wallet "$POLYMARKET_WALLET")
fi

POLYMARKET_OUTPUT="${POLYMARKET_OUTPUT:-polymarket_activity.csv}"
POLYMARKET_LIMIT="${POLYMARKET_LIMIT:-500}"
POLYMARKET_TIMEOUT="${POLYMARKET_TIMEOUT:-30}"
POLYMARKET_POLL_SECONDS="${POLYMARKET_POLL_SECONDS:-10}"
POLYMARKET_FINALIZE_GRACE_SECONDS="${POLYMARKET_FINALIZE_GRACE_SECONDS:-20}"
POLYMARKET_DISCOVERY_PAGES="${POLYMARKET_DISCOVERY_PAGES:-2}"
POLYMARKET_STATE_FILE="${POLYMARKET_STATE_FILE:-polymarket_continuous_state.json}"
POLYMARKET_TELEGRAM_BATCH_SIZE="${POLYMARKET_TELEGRAM_BATCH_SIZE:-10}"

extra_args=()
if [[ -n "${POLYMARKET_CONTINUOUS_MAX_MARKETS:-}" ]]; then
  extra_args+=(--continuous-max-markets "$POLYMARKET_CONTINUOUS_MAX_MARKETS")
fi
if [[ "${POLYMARKET_TELEGRAM_SEND_EXISTING:-0}" == "1" ]]; then
  extra_args+=(--telegram-send-existing)
fi
if [[ "${POLYMARKET_NO_ANALYSIS:-0}" == "1" ]]; then
  extra_args+=(--no-analysis)
fi
if [[ "${POLYMARKET_NO_TELEGRAM_CONTROL:-0}" == "1" ]]; then
  extra_args+=(--no-telegram-control)
fi

exec ".venv/bin/python" "polymarket_activity_to_csv.py" \
  --continuous \
  "${wallet_args[@]}" \
  --output "$POLYMARKET_OUTPUT" \
  --limit "$POLYMARKET_LIMIT" \
  --timeout "$POLYMARKET_TIMEOUT" \
  --poll-seconds "$POLYMARKET_POLL_SECONDS" \
  --finalize-grace-seconds "$POLYMARKET_FINALIZE_GRACE_SECONDS" \
  --continuous-discovery-pages "$POLYMARKET_DISCOVERY_PAGES" \
  --continuous-state-file "$POLYMARKET_STATE_FILE" \
  --telegram-bot-token "$TELEGRAM_BOT_TOKEN" \
  --telegram-chat-id "$TELEGRAM_CHAT_ID" \
  --telegram-batch-size "$POLYMARKET_TELEGRAM_BATCH_SIZE" \
  "${extra_args[@]}"
