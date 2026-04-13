# Polymarket Activity Scraper (VPS)

This scraper can run continuously, export one report per market, and send Telegram ZIP batches where **1 ZIP = 10 markets**.

It supports:

- Multiple wallets in parallel (`POLYMARKET_WALLETS="wallet1,wallet2,wallet3"`)
- Per-wallet market filters (for example `bitcoin`, `ethereum`, `xrp`)
- Telegram command control to update targets at runtime
- Human-readable Telegram ZIP layout grouped as `wallet nickname -> market/time -> files`
  (falls back to full wallet address when nickname is unavailable)
- CSV export appends one trailing `SUMMARY` row with realized PnL and inferred winner

## Quick run (foreground)

```bash
cd /path/to/ActivityScraper
chmod +x run_vps.sh run_service.sh install_service.sh
bash run_vps.sh
```

`run_vps.sh` will ask interactively for:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- optional initial wallets

## One-command service install (keeps running after SSH disconnect)

```bash
cd /path/to/ActivityScraper
chmod +x run_vps.sh run_service.sh install_service.sh
bash install_service.sh
```

`install_service.sh` also prompts interactively for Telegram credentials and optional initial wallets.
You can leave wallets empty and add them later via Telegram commands.

After install:

- Logs: `sudo journalctl -u polymarket-activity.service -f`
- Status: `sudo systemctl status polymarket-activity.service`
- Restart: `sudo systemctl restart polymarket-activity.service`
- Stop: `sudo systemctl stop polymarket-activity.service`

## Optional env vars

- `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` (required, but can be entered interactively in `.sh` scripts)
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
- `POLYMARKET_WALLET_MARKET_FILTERS="vidarx=bitcoin,ethereum;trader2=xrp"` for startup per-wallet filters

## Telegram wallet control commands

The bot sends a reply-keyboard with buttons:

- `Wallets`
- `Select Wallet`
- `Add Wallet`
- `Remove Wallet`
- `Set Wallets`
- `Add Filter`
- `Remove Filter`
- `Help`
- `Cancel`

Filter workflow:

- Press `Select Wallet`
- Choose the tracked wallet nickname/address
- Press `Add Filter` or `Remove Filter`
- Send one or more comma-separated market keywords (for example `bitcoin,ethereum`)

You can still send commands manually:

- `/wallets` - list tracked wallets
- `/wallet_select <wallet_or_nickname>` - select tracked wallet for filter actions
- `/wallet_add <wallet_or_username>` - add a target wallet
- `/wallet_remove <wallet_or_username>` - remove wallet from tracking
- `/wallet_set <w1,w2,w3>` - replace the full wallet list
- `/wallet_filter_add <bitcoin,ethereum>` - add market filters to the selected wallet
- `/wallet_filter_remove <bitcoin>` - remove market filters from the selected wallet
- `/wallet_help` - show command help
- `/cancel` - cancel pending button action

When you add a wallet by address (`0x...`), the bot tries to resolve and use the Polymarket profile name
as the wallet nickname automatically (when available).

Note: command control uses Telegram `getUpdates` polling (not webhook mode).
Only commands from your configured `TELEGRAM_CHAT_ID` are applied.
