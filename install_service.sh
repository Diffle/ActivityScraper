#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="polymarket-activity.service"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${ROOT_DIR}/.service.env"
UNIT_PATH="/etc/systemd/system/${SERVICE_NAME}"
SERVICE_USER="${SUDO_USER:-${USER}}"

if ! command -v python3 >/dev/null 2>&1; then
  echo "Error: python3 is required." >&2
  exit 1
fi

if ! command -v systemctl >/dev/null 2>&1; then
  echo "Error: systemd/systemctl is required on this VPS." >&2
  exit 1
fi

if [[ $EUID -ne 0 ]] && ! command -v sudo >/dev/null 2>&1; then
  echo "Error: sudo is required to install a system service." >&2
  exit 1
fi

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

cd "$ROOT_DIR"

if [[ ! -d ".venv" ]]; then
  python3 -m venv .venv
fi

# shellcheck disable=SC1091
source ".venv/bin/activate"
python -m pip install --upgrade pip >/dev/null
python -m pip install -r requirements.txt >/dev/null
deactivate

chmod +x "${ROOT_DIR}/run_service.sh" "${ROOT_DIR}/run_vps.sh"

POLYMARKET_OUTPUT="${POLYMARKET_OUTPUT:-polymarket_activity.csv}"
POLYMARKET_WALLETS="${POLYMARKET_WALLETS:-}"
POLYMARKET_WALLET="${POLYMARKET_WALLET:-}"
POLYMARKET_LIMIT="${POLYMARKET_LIMIT:-500}"
POLYMARKET_TIMEOUT="${POLYMARKET_TIMEOUT:-30}"
POLYMARKET_POLL_SECONDS="${POLYMARKET_POLL_SECONDS:-10}"
POLYMARKET_FINALIZE_GRACE_SECONDS="${POLYMARKET_FINALIZE_GRACE_SECONDS:-20}"
POLYMARKET_DISCOVERY_PAGES="${POLYMARKET_DISCOVERY_PAGES:-2}"
POLYMARKET_STATE_FILE="${POLYMARKET_STATE_FILE:-polymarket_continuous_state.json}"
POLYMARKET_TELEGRAM_BATCH_SIZE="${POLYMARKET_TELEGRAM_BATCH_SIZE:-10}"
POLYMARKET_TELEGRAM_SEND_EXISTING="${POLYMARKET_TELEGRAM_SEND_EXISTING:-0}"
POLYMARKET_NO_TELEGRAM_CONTROL="${POLYMARKET_NO_TELEGRAM_CONTROL:-0}"
POLYMARKET_NO_ANALYSIS="${POLYMARKET_NO_ANALYSIS:-0}"
POLYMARKET_CONTINUOUS_MAX_MARKETS="${POLYMARKET_CONTINUOUS_MAX_MARKETS:-}"

cat >"${ENV_FILE}" <<EOF
POLYMARKET_WALLET=${POLYMARKET_WALLET}
POLYMARKET_WALLETS=${POLYMARKET_WALLETS}
TELEGRAM_BOT_TOKEN=${TELEGRAM_BOT_TOKEN}
TELEGRAM_CHAT_ID=${TELEGRAM_CHAT_ID}
POLYMARKET_OUTPUT=${POLYMARKET_OUTPUT}
POLYMARKET_LIMIT=${POLYMARKET_LIMIT}
POLYMARKET_TIMEOUT=${POLYMARKET_TIMEOUT}
POLYMARKET_POLL_SECONDS=${POLYMARKET_POLL_SECONDS}
POLYMARKET_FINALIZE_GRACE_SECONDS=${POLYMARKET_FINALIZE_GRACE_SECONDS}
POLYMARKET_DISCOVERY_PAGES=${POLYMARKET_DISCOVERY_PAGES}
POLYMARKET_STATE_FILE=${POLYMARKET_STATE_FILE}
POLYMARKET_TELEGRAM_BATCH_SIZE=${POLYMARKET_TELEGRAM_BATCH_SIZE}
POLYMARKET_TELEGRAM_SEND_EXISTING=${POLYMARKET_TELEGRAM_SEND_EXISTING}
POLYMARKET_NO_TELEGRAM_CONTROL=${POLYMARKET_NO_TELEGRAM_CONTROL}
POLYMARKET_NO_ANALYSIS=${POLYMARKET_NO_ANALYSIS}
POLYMARKET_CONTINUOUS_MAX_MARKETS=${POLYMARKET_CONTINUOUS_MAX_MARKETS}
EOF

chmod 600 "${ENV_FILE}"

SUDO_CMD=""
if [[ $EUID -ne 0 ]]; then
  SUDO_CMD="sudo"
fi

${SUDO_CMD} tee "${UNIT_PATH}" >/dev/null <<EOF
[Unit]
Description=Polymarket Activity Scraper
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${SERVICE_USER}
WorkingDirectory=${ROOT_DIR}
EnvironmentFile=${ENV_FILE}
ExecStart=${ROOT_DIR}/run_service.sh
Restart=always
RestartSec=10
KillSignal=SIGINT
TimeoutStopSec=45

[Install]
WantedBy=multi-user.target
EOF

${SUDO_CMD} systemctl daemon-reload
${SUDO_CMD} systemctl enable --now "${SERVICE_NAME}"

echo "Service installed and started: ${SERVICE_NAME}"
echo "View logs: sudo journalctl -u ${SERVICE_NAME} -f"
echo "Service status: sudo systemctl status ${SERVICE_NAME}"
