#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

if ! command -v python3 >/dev/null 2>&1; then
  echo "Error: python3 is required." >&2
  exit 1
fi

if [[ ! -d ".venv" ]]; then
  python3 -m venv .venv
fi

# shellcheck disable=SC1091
source ".venv/bin/activate"

python -m pip install --upgrade pip >/dev/null
python -m pip install -r requirements.txt >/dev/null

if [[ -z "${TELEGRAM_BOT_TOKEN:-}" && -t 0 ]]; then
  read -r -s -p "Enter TELEGRAM_BOT_TOKEN: " TELEGRAM_BOT_TOKEN
  echo
fi
if [[ -z "${TELEGRAM_CHAT_ID:-}" && -t 0 ]]; then
  read -r -p "Enter TELEGRAM_CHAT_ID: " TELEGRAM_CHAT_ID
fi

if [[ -z "${TELEGRAM_BOT_TOKEN:-}" ]]; then
  echo "Error: missing TELEGRAM_BOT_TOKEN" >&2
  exit 1
fi
if [[ -z "${TELEGRAM_CHAT_ID:-}" ]]; then
  echo "Error: missing TELEGRAM_CHAT_ID" >&2
  exit 1
fi

if [[ -z "${POLYMARKET_WALLETS:-}" && -z "${POLYMARKET_WALLET:-}" && -t 0 ]]; then
  read -r -p "Optional initial wallets (comma-separated, leave blank to add via Telegram): " POLYMARKET_WALLETS
fi

wallet_args=()
if [[ -n "${POLYMARKET_WALLETS:-}" ]]; then
  wallet_args+=(--wallets "$POLYMARKET_WALLETS")
elif [[ -n "${POLYMARKET_WALLET:-}" ]]; then
  wallet_args+=(--wallet "$POLYMARKET_WALLET")
else
  echo "Info: no initial wallets set, bot will wait for /wallet_add or /wallet_set in Telegram." >&2
fi

POLYMARKET_OUTPUT="${POLYMARKET_OUTPUT:-polymarket_activity.csv}"
POLYMARKET_LIMIT="${POLYMARKET_LIMIT:-500}"
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

exec python polymarket_activity_to_csv.py \
  --continuous \
  "${wallet_args[@]}" \
  --output "$POLYMARKET_OUTPUT" \
  --limit "$POLYMARKET_LIMIT" \
  --poll-seconds "$POLYMARKET_POLL_SECONDS" \
  --finalize-grace-seconds "$POLYMARKET_FINALIZE_GRACE_SECONDS" \
  --continuous-discovery-pages "$POLYMARKET_DISCOVERY_PAGES" \
  --continuous-state-file "$POLYMARKET_STATE_FILE" \
  --telegram-bot-token "$TELEGRAM_BOT_TOKEN" \
  --telegram-chat-id "$TELEGRAM_CHAT_ID" \
  --telegram-batch-size "$POLYMARKET_TELEGRAM_BATCH_SIZE" \
  "${extra_args[@]}"
