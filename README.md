# Polymarket Activity Scraper (VPS)

This scraper can run continuously, export one report per market, and send Telegram ZIP batches where **1 ZIP = 10 markets**.

It supports:

- Multiple wallets in parallel (`POLYMARKET_WALLETS="wallet1,wallet2,wallet3"`)
- Telegram command control to update targets at runtime

## Quick run (foreground)

```bash
cd /path/to/ActivityScraper
chmod +x run_vps.sh run_service.sh install_service.sh
POLYMARKET_WALLETS="vidarx,another_wallet" \
TELEGRAM_BOT_TOKEN="123456:ABCDEF" \
TELEGRAM_CHAT_ID="-1001234567890" \
bash run_vps.sh
```

## One-command service install (keeps running after SSH disconnect)

```bash
cd /path/to/ActivityScraper
chmod +x run_vps.sh run_service.sh install_service.sh
POLYMARKET_WALLETS="vidarx,another_wallet" \
TELEGRAM_BOT_TOKEN="123456:ABCDEF" \
TELEGRAM_CHAT_ID="-1001234567890" \
bash install_service.sh
```

After install:

- Logs: `sudo journalctl -u polymarket-activity.service -f`
- Status: `sudo systemctl status polymarket-activity.service`
- Restart: `sudo systemctl restart polymarket-activity.service`
- Stop: `sudo systemctl stop polymarket-activity.service`

## Optional env vars

- `POLYMARKET_WALLET` (single wallet fallback)
- `POLYMARKET_WALLETS` (comma-separated multi-wallet list)
- `POLYMARKET_TELEGRAM_BATCH_SIZE` (default `10`)
- `POLYMARKET_POLL_SECONDS` (default `10`)
- `POLYMARKET_FINALIZE_GRACE_SECONDS` (default `20`)
- `POLYMARKET_LIMIT` (default `500`)
- `POLYMARKET_STATE_FILE` (default `polymarket_continuous_state.json`)
- `POLYMARKET_TELEGRAM_SEND_EXISTING=1` to send old unsent exports from state on startup
- `POLYMARKET_NO_TELEGRAM_CONTROL=1` to disable Telegram wallet management commands
- `POLYMARKET_NO_ANALYSIS=1` to skip analysis/scenario files

## Telegram wallet control commands

Send these to your bot in the configured chat:

- `/wallets` - list tracked wallets
- `/wallet_add <wallet_or_username>` - add a target wallet
- `/wallet_remove <wallet_or_username>` - remove wallet from tracking
- `/wallet_set <w1,w2,w3>` - replace the full wallet list
- `/wallet_help` - show command help

Note: command control uses Telegram `getUpdates` polling (not webhook mode).
Only commands from your configured `TELEGRAM_CHAT_ID` are applied.
