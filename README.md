# Polymarket Activity Scraper (VPS)

This scraper can run continuously, export one report per market, and send Telegram ZIP batches where **1 ZIP = 10 markets**.

## Quick run (foreground)

```bash
cd /path/to/ActivityScraper
chmod +x run_vps.sh run_service.sh install_service.sh
POLYMARKET_WALLET="vidarx" \
TELEGRAM_BOT_TOKEN="123456:ABCDEF" \
TELEGRAM_CHAT_ID="-1001234567890" \
bash run_vps.sh
```

## One-command service install (keeps running after SSH disconnect)

```bash
cd /path/to/ActivityScraper
chmod +x run_vps.sh run_service.sh install_service.sh
POLYMARKET_WALLET="vidarx" \
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

- `POLYMARKET_TELEGRAM_BATCH_SIZE` (default `10`)
- `POLYMARKET_POLL_SECONDS` (default `10`)
- `POLYMARKET_FINALIZE_GRACE_SECONDS` (default `20`)
- `POLYMARKET_LIMIT` (default `500`)
- `POLYMARKET_STATE_FILE` (default `polymarket_continuous_state.json`)
- `POLYMARKET_TELEGRAM_SEND_EXISTING=1` to send old unsent exports from state on startup
- `POLYMARKET_NO_ANALYSIS=1` to skip analysis/scenario files
