#!/usr/bin/env python3
"""Export Polymarket wallet activity to CSV.

Usage:
  python polymarket_activity_to_csv.py
  python polymarket_activity_to_csv.py --wallet 0xabc... -o activity.csv
  python polymarket_activity_to_csv.py --wallet vidarx -o activity.csv
  python polymarket_activity_to_csv.py --wallet vidarx --types TRADE,REDEEM --side BUY
  python polymarket_activity_to_csv.py --wallet vidarx --start 2026-04-01 --end 2026-04-08
  python polymarket_activity_to_csv.py --wallet vidarx \
      --market-title "Bitcoin Up or Down - April 7, 6:00PM-6:05PM ET" \
      -o one_market.csv
  python polymarket_activity_to_csv.py --wallet vidarx --continuous -o polymarket_activity.csv
  python polymarket_activity_to_csv.py --wallet vidarx --continuous \
      --telegram-bot-token 123456:ABC --telegram-chat-id -1001234567890
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urljoin

import requests

DATA_API = "https://data-api.polymarket.com/"
GAMMA_API = "https://gamma-api.polymarket.com/"
WALLET_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
ACTIVITY_OFFSET_CAP = 3500
CONTINUOUS_DEFAULT_STATE_FILE = "polymarket_continuous_state.json"
CONTINUOUS_DEFAULT_POLL_SECONDS = 10
CONTINUOUS_DEFAULT_FINALIZE_GRACE_SECONDS = 20
CONTINUOUS_DEFAULT_DISCOVERY_PAGES = 2
CONTINUOUS_ACTIVITY_TYPES = ["TRADE", "REDEEM"]
TELEGRAM_DEFAULT_BATCH_SIZE = 10
TELEGRAM_DEFAULT_SEND_TIMEOUT_SECONDS = 60
TELEGRAM_STATE_KEY = "telegram"
TELEGRAM_SENT_BATCHES_LIMIT = 200
TELEGRAM_SEND_DOCUMENT_MAX_BYTES = 50 * 1024 * 1024
CSV_COLUMNS = [
    "datetime_utc",
    "timestamp",
    "type",
    "side",
    "usdcSize",
    "size",
    "price",
    "outcome",
    "outcomeIndex",
    "title",
]
DEFAULT_SCENARIO_MIN_BETS = [0.0, 5.0, 10.0, 20.0]
DEFAULT_SCENARIO_MAX_BETS = [20.0, 40.0, 60.0, 100.0, 1_000_000.0]
DEFAULT_SCENARIO_MAX_PRICES_INPUT = "auto"
AUTO_GRID_MAX_LEVELS_DEFAULT = 60
AUTO_GRID_MAX_LEVELS_MID = 45
AUTO_GRID_MAX_LEVELS_HIGH = 30
REFERENCE_MIN_BET_USDC = 1.0
REFERENCE_MAX_BET_USDC = 40.0
REFERENCE_MAX_PRICE = 0.6
REFERENCE_2_MIN_BET_USDC = 0.0
REFERENCE_2_MAX_BET_USDC = 20.0
REFERENCE_2_MAX_PRICE = 0.6
REFERENCE_3_MIN_BET_USDC = 10.0
REFERENCE_3_MAX_BET_USDC = 20.0
REFERENCE_3_MAX_PRICE = 1.0
INVALID_FILENAME_CHARS_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f]+')
MARKET_FILENAME_MAX_LEN = 90


def is_wallet(value: str) -> bool:
    return bool(WALLET_RE.fullmatch(value.strip()))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch a Polymarket wallet's public activity and export it to CSV."
    )
    parser.add_argument(
        "--wallet",
        default=os.getenv("POLYMARKET_WALLET"),
        help=(
            "Proxy wallet address (0x...) or a public Polymarket username/pseudonym. "
            "If omitted, you'll be prompted. Environment fallback: POLYMARKET_WALLET."
        ),
    )
    parser.add_argument(
        "-o",
        "--output",
        default="polymarket_activity.csv",
        help=(
            "Output CSV path (default: polymarket_activity.csv). "
            "If one market is selected, its title is appended to the filename."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Page size for API requests, max 500 (default: 500)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Stop after this many pages. Omit to fetch everything.",
    )
    parser.add_argument(
        "--types",
        default=None,
        help=(
            "Comma-separated activity types, e.g. TRADE,REDEEM. "
            "Allowed values include TRADE,SPLIT,MERGE,REDEEM,REWARD,CONVERSION,"
            "MAKER_REBATE,REFERRAL_REWARD."
        ),
    )
    parser.add_argument(
        "--side",
        choices=["BUY", "SELL"],
        default=None,
        help="Filter by side for trade-like events.",
    )
    parser.add_argument(
        "--start",
        default=None,
        help="UTC start date/time. Examples: 2026-04-01 or 2026-04-01T00:00:00",
    )
    parser.add_argument(
        "--end",
        default=None,
        help="UTC end date/time. Examples: 2026-04-08 or 2026-04-08T23:59:59",
    )
    parser.add_argument(
        "--market-title",
        default=None,
        help=(
            "Only export activity for one market title. Example: "
            '"Bitcoin Up or Down - April 7, 6:00PM-6:05PM ET"'
        ),
    )
    parser.add_argument(
        "--no-interactive",
        action="store_true",
        help="Disable interactive market picker when --market-title is omitted.",
    )
    parser.add_argument(
        "--recent-markets",
        type=int,
        default=5,
        help="How many recent markets to show in interactive picker (default: 5)",
    )
    parser.add_argument(
        "--market-match",
        choices=["exact", "contains"],
        default="exact",
        help=(
            "How to match --market-title: exact (default) or contains. "
            "Matching is case-insensitive and ignores repeated whitespace."
        ),
    )
    parser.add_argument(
        "--sort-by",
        choices=["TIMESTAMP", "TOKENS", "CASH"],
        default="TIMESTAMP",
        help="Sorting field for the activity endpoint (default: TIMESTAMP)",
    )
    parser.add_argument(
        "--sort-direction",
        choices=["ASC", "DESC"],
        default="DESC",
        help="Sort order (default: DESC)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP timeout in seconds (default: 30)",
    )
    parser.add_argument(
        "--no-analysis",
        action="store_true",
        help="Skip generating analysis report and copy-setting scenarios.",
    )
    parser.add_argument(
        "--scenario-min-bets",
        default=",".join(str(v) for v in DEFAULT_SCENARIO_MIN_BETS),
        help=(
            "Comma-separated min bet sizes in USDC for scenario simulation. "
            "Supports ranges like 0:20:5 or 'auto'."
        ),
    )
    parser.add_argument(
        "--scenario-max-bets",
        default=",".join(str(v) for v in DEFAULT_SCENARIO_MAX_BETS),
        help=(
            "Comma-separated max bet sizes in USDC for scenario simulation. "
            "Supports ranges like 20:100:20 or 'auto'."
        ),
    )
    parser.add_argument(
        "--scenario-max-prices",
        default=DEFAULT_SCENARIO_MAX_PRICES_INPUT,
        help=(
            "Max share prices for scenario simulation. Use 'auto' (default) to test all observed prices, "
            "or pass comma-separated values/ranges like 0.5,0.6,0.7 or 0.1:1.0:0.05."
        ),
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help=(
            "Run continuously: track the next active market with wallet activity, "
            "collect TRADE (BUY/SELL) + REDEEM rows, export files when the market closes, "
            "then move to the next market."
        ),
    )
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=CONTINUOUS_DEFAULT_POLL_SECONDS,
        help=f"Continuous mode poll interval in seconds (default: {CONTINUOUS_DEFAULT_POLL_SECONDS})",
    )
    parser.add_argument(
        "--finalize-grace-seconds",
        type=int,
        default=CONTINUOUS_DEFAULT_FINALIZE_GRACE_SECONDS,
        help=(
            "After market closes, wait this many seconds before final export "
            f"(default: {CONTINUOUS_DEFAULT_FINALIZE_GRACE_SECONDS})."
        ),
    )
    parser.add_argument(
        "--continuous-discovery-pages",
        type=int,
        default=CONTINUOUS_DEFAULT_DISCOVERY_PAGES,
        help=(
            "How many recent activity pages to scan when choosing the next active market "
            f"(default: {CONTINUOUS_DEFAULT_DISCOVERY_PAGES})."
        ),
    )
    parser.add_argument(
        "--continuous-max-markets",
        type=int,
        default=None,
        help="Stop after exporting this many markets in continuous mode.",
    )
    parser.add_argument(
        "--continuous-state-file",
        default=CONTINUOUS_DEFAULT_STATE_FILE,
        help=(
            "Path to continuous mode state JSON (processed condition IDs). "
            f"Default: {CONTINUOUS_DEFAULT_STATE_FILE}"
        ),
    )
    parser.add_argument(
        "--telegram-bot-token",
        default=None,
        help=(
            "Telegram bot token for report delivery. "
            "Environment fallback: TELEGRAM_BOT_TOKEN."
        ),
    )
    parser.add_argument(
        "--telegram-chat-id",
        default=None,
        help=(
            "Telegram chat ID or channel username (e.g. -100... or @channel). "
            "Environment fallback: TELEGRAM_CHAT_ID."
        ),
    )
    parser.add_argument(
        "--telegram-batch-size",
        type=int,
        default=TELEGRAM_DEFAULT_BATCH_SIZE,
        help=(
            "How many exported markets to pack into one Telegram ZIP file "
            f"(default: {TELEGRAM_DEFAULT_BATCH_SIZE})."
        ),
    )
    parser.add_argument(
        "--telegram-send-existing",
        action="store_true",
        help=(
            "When Telegram is enabled, also send old unsent exports from state file. "
            "By default only new exports are sent."
        ),
    )
    return parser.parse_args()


def parse_utc_to_unix(value: Optional[str]) -> Optional[int]:
    if not value:
        return None

    raw = value.strip().replace("Z", "+00:00")
    formats = (
        None,
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    )

    dt: Optional[datetime] = None
    for fmt in formats:
        try:
            if fmt is None:
                dt = datetime.fromisoformat(raw)
            else:
                dt = datetime.strptime(raw, fmt)
            break
        except ValueError:
            continue

    if dt is None:
        raise ValueError(f"Could not parse datetime: {value!r}")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return int(dt.timestamp())


def session_with_headers() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (compatible; polymarket-activity-exporter/1.1)",
            "Accept": "application/json",
        }
    )
    return session


def resolve_wallet(session: requests.Session, identifier: str, timeout: int) -> str:
    identifier = identifier.strip()
    if is_wallet(identifier):
        return identifier

    url = urljoin(GAMMA_API, "public-search")
    params = {
        "q": identifier,
        "search_profiles": "true",
        "limit_per_type": 10,
        "page": 1,
    }
    resp = session.get(url, params=params, timeout=timeout)
    resp.raise_for_status()
    payload = resp.json()
    profiles = payload.get("profiles") or []
    if not profiles:
        raise RuntimeError(f"No Polymarket profile matched {identifier!r}")

    def score(profile: Dict[str, Any]) -> tuple[int, int]:
        name = str(profile.get("name") or "")
        pseudonym = str(profile.get("pseudonym") or "")
        target = identifier.lower()
        exact = int(target in {name.lower(), pseudonym.lower()})
        has_wallet = int(bool(profile.get("proxyWallet")))
        return (exact, has_wallet)

    best = max(profiles, key=score)
    wallet = best.get("proxyWallet")
    if not wallet:
        raise RuntimeError(
            f"Matched profile for {identifier!r}, but it did not include a proxy wallet address"
        )
    return str(wallet)


def prompt_for_wallet(identifier: Optional[str]) -> str:
    if identifier and identifier.strip():
        return identifier.strip()

    if not sys.stdin.isatty():
        raise RuntimeError("Missing wallet. Provide --wallet when running non-interactively.")

    try:
        entered = input("Enter wallet address or Polymarket username: ").strip()
    except EOFError as exc:
        raise RuntimeError("Wallet is required. Pass --wallet when input is not available.") from exc
    if not entered:
        raise RuntimeError("Wallet is required.")
    return entered


def fetch_activity(
    session: requests.Session,
    wallet: str,
    page_limit: int,
    timeout: int,
    max_pages: Optional[int] = None,
    types: Optional[List[str]] = None,
    side: Optional[str] = None,
    start_ts: Optional[int] = None,
    end_ts: Optional[int] = None,
    sort_by: str = "TIMESTAMP",
    sort_direction: str = "DESC",
    verbose: bool = True,
) -> List[Dict[str, Any]]:
    page_limit = max(1, min(page_limit, 500))
    url = urljoin(DATA_API, "activity")
    offset = 0
    page = 0
    all_rows: List[Dict[str, Any]] = []

    while True:
        page += 1
        params: Dict[str, Any] = {
            "user": wallet,
            "limit": page_limit,
            "offset": offset,
            "sortBy": sort_by,
            "sortDirection": sort_direction,
        }
        if types:
            params["type"] = ",".join(types)
        if side:
            params["side"] = side
        if start_ts is not None:
            params["start"] = start_ts
        if end_ts is not None:
            params["end"] = end_ts

        resp = session.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        batch = resp.json()
        if not isinstance(batch, list):
            raise RuntimeError(f"Unexpected /activity response shape: {type(batch)!r}")

        if not batch:
            break

        all_rows.extend(batch)
        if verbose:
            print(f"Fetched page {page} ({len(batch)} rows, total {len(all_rows)})", file=sys.stderr)

        if len(batch) < page_limit:
            break
        if max_pages is not None and page >= max_pages:
            break
        if offset + page_limit >= ACTIVITY_OFFSET_CAP:
            if verbose:
                print(
                    (
                        f"Reached API pagination cap at offset {ACTIVITY_OFFSET_CAP}. "
                        "Returning partial history. Use --start/--end to narrow the window."
                    ),
                    file=sys.stderr,
                )
            break

        offset += page_limit
        time.sleep(0.15)

    return all_rows


def normalize_for_match(value: Optional[str]) -> str:
    text = str(value or "")
    return " ".join(text.split()).strip().casefold()


def sanitize_filename_component(value: str, max_len: int = MARKET_FILENAME_MAX_LEN) -> str:
    cleaned = " ".join(str(value or "").split()).strip()
    if not cleaned:
        return "market"

    cleaned = cleaned.replace(":", "-")
    cleaned = INVALID_FILENAME_CHARS_RE.sub("-", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .")
    if not cleaned:
        return "market"

    if len(cleaned) > max_len:
        cleaned = cleaned[:max_len].rstrip(" .-")

    return cleaned or "market"


def clean_market_titles(market_titles: Optional[Iterable[str]]) -> List[str]:
    cleaned: List[str] = []
    seen: set[str] = set()
    for title in market_titles or []:
        raw = str(title or "").strip()
        if not raw:
            continue
        key = normalize_for_match(raw)
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(raw)
    return cleaned


def output_path_with_market_label(output_path: str, market_titles: Optional[Iterable[str]]) -> str:
    cleaned_titles = clean_market_titles(market_titles)
    if not cleaned_titles:
        return output_path

    if len(cleaned_titles) == 1:
        raw_label = cleaned_titles[0]
    else:
        raw_label = f"{len(cleaned_titles)} markets"

    market_label = sanitize_filename_component(raw_label)
    path = Path(output_path)
    stem = path.stem
    if normalize_for_match(market_label) in normalize_for_match(stem):
        return output_path

    tagged_stem = f"{stem} - {market_label}"
    tagged_name = tagged_stem + path.suffix
    return str(path.with_name(tagged_name))


def filter_rows_by_market_titles(
    rows: Iterable[Dict[str, Any]],
    market_titles: Optional[Iterable[str]],
    match_mode: str = "exact",
) -> List[Dict[str, Any]]:
    rows_list = list(rows)
    cleaned_titles = clean_market_titles(market_titles)
    if not cleaned_titles:
        return rows_list

    targets = [normalize_for_match(title) for title in cleaned_titles]
    targets = [target for target in targets if target]
    if not targets:
        return rows_list
    target_set = set(targets)

    filtered: List[Dict[str, Any]] = []
    for row in rows_list:
        row_title = normalize_for_match(row.get("title"))
        if match_mode == "exact":
            keep = row_title in target_set
        else:
            keep = any(target in row_title for target in targets)
        if keep:
            filtered.append(row)

    return filtered


def summarize_market_titles(rows: Iterable[Dict[str, Any]], limit: int = 10) -> List[str]:
    counts: Dict[str, int] = {}
    display_names: Dict[str, str] = {}
    for row in rows:
        raw_title = str(row.get("title") or "").strip()
        if not raw_title:
            continue
        key = normalize_for_match(raw_title)
        counts[key] = counts.get(key, 0) + 1
        display_names.setdefault(key, raw_title)

    ranked = sorted(counts.items(), key=lambda item: (-item[1], display_names[item[0]]))
    return [f"{display_names[key]} ({count})" for key, count in ranked[:limit]]


def recent_market_choices(rows: Iterable[Dict[str, Any]], limit: int = 5) -> List[Dict[str, Any]]:
    counts: Dict[str, int] = {}
    display_names: Dict[str, str] = {}
    latest_ts: Dict[str, int] = {}

    for row in rows:
        raw_title = str(row.get("title") or "").strip()
        if not raw_title:
            continue
        key = normalize_for_match(raw_title)
        counts[key] = counts.get(key, 0) + 1
        display_names.setdefault(key, raw_title)
        try:
            ts_value = int(row.get("timestamp"))
        except (TypeError, ValueError):
            ts_value = -1
        latest_ts[key] = max(latest_ts.get(key, -1), ts_value)

    ranked = sorted(latest_ts.items(), key=lambda item: (-item[1], display_names[item[0]]))
    choices: List[Dict[str, Any]] = []
    for key, ts_value in ranked[: max(1, limit)]:
        choices.append(
            {
                "title": display_names[key],
                "count": counts[key],
                "latest_ts": ts_value,
            }
        )
    return choices


def parse_market_selection(raw: str, max_choice: int) -> Optional[List[int]]:
    parts = [part.strip() for part in raw.split(",") if part.strip()]
    if not parts:
        return None

    picked: List[int] = []
    seen: set[int] = set()
    for part in parts:
        if not part.isdigit():
            return None
        value = int(part)
        if value < 0 or value > max_choice:
            return None
        if value == 0:
            if len(parts) > 1:
                return None
            return [0]
        if value in seen:
            continue
        seen.add(value)
        picked.append(value)

    return picked


def choose_market_interactively(rows: Iterable[Dict[str, Any]], limit: int = 5) -> Optional[List[str]]:
    choices = recent_market_choices(rows, limit=limit)
    if not choices:
        print("No recent market titles found in the latest activity.", file=sys.stderr)
        return None

    print("Recent markets (newest first):", file=sys.stderr)
    for idx, choice in enumerate(choices, start=1):
        latest = unix_to_iso(choice.get("latest_ts"))
        print(
            f"  {idx}) {choice['title']} ({choice['count']} rows, latest {latest})",
            file=sys.stderr,
        )
    print("  0) All markets", file=sys.stderr)

    prompt = f"Select market(s) [0-{len(choices)}] (default 1): "
    while True:
        try:
            raw = input(prompt).strip()
        except EOFError:
            return None

        if raw == "":
            return [str(choices[0]["title"])]

        picked = parse_market_selection(raw, len(choices))
        if picked is None:
            print(
                f"Please enter 0, one number, or comma-separated numbers like 1,2 (0-{len(choices)}).",
                file=sys.stderr,
            )
            continue
        if picked == [0]:
            return None
        if picked:
            return [str(choices[index - 1]["title"]) for index in picked]

        print(
            f"Please enter 0, one number, or comma-separated numbers like 1,2 (0-{len(choices)}).",
            file=sys.stderr,
        )


def normalize_condition_id(value: Any) -> str:
    return str(value or "").strip().lower()


def filter_rows_by_condition_id(
    rows: Iterable[Dict[str, Any]],
    condition_id: str,
) -> List[Dict[str, Any]]:
    target = normalize_condition_id(condition_id)
    if not target:
        return []
    return [
        row
        for row in rows
        if normalize_condition_id(row.get("conditionId")) == target
    ]


def recent_condition_choices(
    rows: Iterable[Dict[str, Any]],
    limit: int = 100,
) -> List[Dict[str, Any]]:
    by_condition: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        row_type = str(row.get("type") or "").upper()
        if row_type not in CONTINUOUS_ACTIVITY_TYPES:
            continue

        condition_id = normalize_condition_id(row.get("conditionId"))
        if not condition_id:
            continue

        title = str(row.get("title") or "").strip() or "(untitled market)"
        entry = by_condition.setdefault(
            condition_id,
            {
                "condition_id": condition_id,
                "title": title,
                "latest_ts": -1,
                "row_count": 0,
            },
        )
        entry["row_count"] += 1

        try:
            ts_value = int(row.get("timestamp"))
        except (TypeError, ValueError):
            ts_value = -1

        if ts_value > entry["latest_ts"]:
            entry["latest_ts"] = ts_value
            entry["title"] = title

    ranked = sorted(by_condition.values(), key=lambda item: (-item["latest_ts"], str(item["title"])))
    return ranked[: max(1, limit)]


def fetch_market_by_condition_id(
    session: requests.Session,
    condition_id: str,
    timeout: int,
) -> Optional[Dict[str, Any]]:
    normalized = normalize_condition_id(condition_id)
    if not normalized:
        return None

    url = urljoin(GAMMA_API, "markets")
    params = {
        "condition_ids": normalized,
        "limit": 1,
    }
    resp = session.get(url, params=params, timeout=timeout)
    resp.raise_for_status()
    payload = resp.json()
    if not isinstance(payload, list) or not payload:
        return None
    first = payload[0]
    if not isinstance(first, dict):
        return None
    return first


def market_is_active(market: Optional[Dict[str, Any]]) -> bool:
    if not market:
        return False
    return bool(market.get("active")) and not bool(market.get("closed"))


def market_title_from_metadata(market: Optional[Dict[str, Any]], fallback: str = "") -> str:
    for key in ("question", "title", "slug"):
        if not market:
            break
        value = str(market.get(key) or "").strip()
        if value:
            return value
    return fallback or "(untitled market)"


def market_start_ts_from_metadata(market: Optional[Dict[str, Any]]) -> Optional[int]:
    if not market:
        return None

    for key in ("startDate", "startDateIso", "gameStartTime", "createdAt"):
        raw = market.get(key)
        if not raw:
            continue
        try:
            parsed = parse_utc_to_unix(str(raw))
        except ValueError:
            parsed = None
        if parsed is not None:
            return parsed

    return None


def load_continuous_state(state_path: str) -> Dict[str, Any]:
    default_state = {"processed_condition_ids": []}
    path = Path(state_path)
    if not path.exists():
        return default_state

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(
            f"Warning: failed to read continuous state {path}: {exc}. Starting with empty state.",
            file=sys.stderr,
        )
        return default_state

    if not isinstance(payload, dict):
        print(
            f"Warning: invalid continuous state format in {path}. Starting with empty state.",
            file=sys.stderr,
        )
        return default_state

    raw_ids = payload.get("processed_condition_ids")
    if not isinstance(raw_ids, list):
        raw_ids = []

    seen: set[str] = set()
    cleaned_ids: List[str] = []
    for item in raw_ids:
        normalized = normalize_condition_id(item)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        cleaned_ids.append(normalized)

    state = dict(payload)
    state["processed_condition_ids"] = cleaned_ids
    return state


def save_continuous_state(state_path: str, state: Dict[str, Any]) -> None:
    path = Path(state_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    payload = dict(state)
    payload["updated_utc"] = datetime.now(timezone.utc).isoformat()
    raw_ids = payload.get("processed_condition_ids")
    if not isinstance(raw_ids, list):
        payload["processed_condition_ids"] = []

    with path.open("w", encoding="utf-8", newline="") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)
        fh.write("\n")


def resolve_existing_file(path_text: Any) -> Optional[Path]:
    raw = str(path_text or "").strip()
    if not raw:
        return None

    path = Path(raw)
    if path.exists() and path.is_file():
        return path
    return None


def collect_export_report_files(export_item: Dict[str, Any]) -> List[Path]:
    candidates: List[Path] = []
    for key in ("csv_path", "analysis_path", "scenarios_path"):
        file_path = resolve_existing_file(export_item.get(key))
        if file_path is not None:
            candidates.append(file_path)

    if not any(path.name.endswith("_scenarios.csv") for path in candidates):
        inferred_candidates: List[Path] = []

        analysis_path = resolve_existing_file(export_item.get("analysis_path"))
        if analysis_path is not None and analysis_path.name.endswith("_analysis.md"):
            inferred = analysis_path.with_name(
                analysis_path.name.replace("_analysis.md", "_scenarios.csv")
            )
            if inferred.exists() and inferred.is_file():
                inferred_candidates.append(inferred)

        csv_path = resolve_existing_file(export_item.get("csv_path"))
        if csv_path is not None and csv_path.suffix.lower() == ".csv":
            inferred = csv_path.with_name(csv_path.stem + "_scenarios.csv")
            if inferred.exists() and inferred.is_file():
                inferred_candidates.append(inferred)

        candidates.extend(inferred_candidates)

    deduped: List[Path] = []
    seen: set[str] = set()
    for path in candidates:
        key = str(path.resolve())
        if key in seen:
            continue
        seen.add(key)
        deduped.append(path)
    return deduped


def build_telegram_batch_zip(
    exports_batch: List[Dict[str, Any]],
    batch_start_index: int,
    zip_directory: Path,
) -> tuple[Path, int]:
    zip_directory.mkdir(parents=True, exist_ok=True)
    batch_end_index = batch_start_index + len(exports_batch)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    zip_name = (
        f"polymarket_reports_{batch_start_index + 1:05d}-{batch_end_index:05d}_{timestamp}.zip"
    )
    zip_path = zip_directory / zip_name

    attached_count = 0
    manifest_lines: List[str] = [
        f"created_utc={datetime.now(timezone.utc).isoformat()}",
        f"batch_start_index={batch_start_index}",
        f"batch_end_index={batch_end_index}",
        "",
    ]

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for idx, export_item in enumerate(exports_batch, start=1):
            title = str(export_item.get("title") or "market").strip()
            safe_title = sanitize_filename_component(title, max_len=55)
            folder_name = f"{idx:02d}_{safe_title}"
            condition_id = str(export_item.get("condition_id") or "")
            market_id = str(export_item.get("market_id") or "")

            manifest_lines.append(
                f"[{idx}] title={title} | condition_id={condition_id} | market_id={market_id}"
            )

            files = collect_export_report_files(export_item)
            if not files:
                manifest_lines.append("  files=none_found")
                continue

            for file_path in files:
                arcname = f"{folder_name}/{file_path.name}"
                zf.write(file_path, arcname=arcname)
                attached_count += 1
                manifest_lines.append(f"  file={file_path}")

        zf.writestr("manifest.txt", "\n".join(manifest_lines) + "\n")

    return zip_path, attached_count


def send_telegram_document(
    bot_token: str,
    chat_id: str,
    document_path: Path,
    caption: str,
    timeout: int,
) -> Dict[str, Any]:
    file_size = document_path.stat().st_size
    if file_size > TELEGRAM_SEND_DOCUMENT_MAX_BYTES:
        raise RuntimeError(
            (
                f"Telegram sendDocument limit exceeded for {document_path.name}: "
                f"{file_size} bytes > {TELEGRAM_SEND_DOCUMENT_MAX_BYTES} bytes"
            )
        )

    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
    payload = {
        "chat_id": chat_id,
        "caption": caption[:1024],
    }

    with document_path.open("rb") as fh:
        files = {
            "document": (document_path.name, fh, "application/zip"),
        }
        response = requests.post(url, data=payload, files=files, timeout=timeout)

    response.raise_for_status()
    body = response.json()
    if not isinstance(body, dict) or not body.get("ok"):
        raise RuntimeError(f"Telegram API error: {body}")
    return body


def ensure_telegram_state(
    state: Dict[str, Any],
    batch_size: int,
    send_existing: bool,
) -> Dict[str, Any]:
    exports = state.get("exports")
    if not isinstance(exports, list):
        exports = []
        state["exports"] = exports

    telegram_state = state.get(TELEGRAM_STATE_KEY)
    if not isinstance(telegram_state, dict):
        telegram_state = {}

    next_export_index = telegram_state.get("next_export_index")
    if not isinstance(next_export_index, int):
        next_export_index = 0 if send_existing else len(exports)
    next_export_index = max(0, min(next_export_index, len(exports)))

    telegram_state["next_export_index"] = next_export_index
    telegram_state["batch_size"] = batch_size

    sent_batches = telegram_state.get("sent_batches")
    if not isinstance(sent_batches, list):
        telegram_state["sent_batches"] = []

    state[TELEGRAM_STATE_KEY] = telegram_state
    return telegram_state


def flush_telegram_batches(
    state: Dict[str, Any],
    state_path: str,
    bot_token: str,
    chat_id: str,
    batch_size: int,
    timeout: int,
) -> None:
    exports = state.get("exports")
    if not isinstance(exports, list) or not exports:
        return

    telegram_state = state.get(TELEGRAM_STATE_KEY)
    if not isinstance(telegram_state, dict):
        telegram_state = ensure_telegram_state(state, batch_size=batch_size, send_existing=False)

    start_index = telegram_state.get("next_export_index")
    if not isinstance(start_index, int):
        start_index = len(exports)
    start_index = max(0, min(start_index, len(exports)))

    zip_directory = Path(state_path).resolve().parent / "telegram_batches"

    while len(exports) - start_index >= batch_size:
        batch = exports[start_index : start_index + batch_size]
        batch_end = start_index + len(batch)

        zip_path, attached_count = build_telegram_batch_zip(
            exports_batch=batch,
            batch_start_index=start_index,
            zip_directory=zip_directory,
        )

        caption = (
            f"Polymarket reports {start_index + 1}-{batch_end} "
            f"({len(batch)} markets, {attached_count} files)"
        )

        try:
            send_telegram_document(
                bot_token=bot_token,
                chat_id=chat_id,
                document_path=zip_path,
                caption=caption,
                timeout=max(5, timeout),
            )
        except Exception as exc:
            telegram_state["last_error"] = str(exc)
            state[TELEGRAM_STATE_KEY] = telegram_state
            save_continuous_state(state_path, state)
            print(
                (
                    f"Warning: failed to send Telegram batch {start_index + 1}-{batch_end}: {exc}. "
                    "Will retry after next export/poll."
                ),
                file=sys.stderr,
            )
            return

        sent_batches = telegram_state.get("sent_batches")
        if not isinstance(sent_batches, list):
            sent_batches = []
        sent_batches.append(
            {
                "range_start": start_index + 1,
                "range_end": batch_end,
                "markets": len(batch),
                "zip_path": str(zip_path),
                "sent_utc": datetime.now(timezone.utc).isoformat(),
            }
        )
        telegram_state["sent_batches"] = sent_batches[-TELEGRAM_SENT_BATCHES_LIMIT:]
        telegram_state["next_export_index"] = batch_end
        telegram_state["last_sent_utc"] = datetime.now(timezone.utc).isoformat()
        telegram_state.pop("last_error", None)

        state[TELEGRAM_STATE_KEY] = telegram_state
        save_continuous_state(state_path, state)

        print(
            (
                f"Sent Telegram batch {start_index + 1}-{batch_end} "
                f"as {zip_path.name} ({attached_count} files)."
            ),
            file=sys.stderr,
        )

        start_index = batch_end


def choose_next_active_market(
    session: requests.Session,
    wallet: str,
    timeout: int,
    page_limit: int,
    discovery_pages: int,
    processed_condition_ids: Iterable[str],
    start_ts: Optional[int],
    end_ts: Optional[int],
) -> Optional[Dict[str, Any]]:
    preview_rows = fetch_activity(
        session=session,
        wallet=wallet,
        page_limit=page_limit,
        timeout=timeout,
        max_pages=max(1, discovery_pages),
        types=CONTINUOUS_ACTIVITY_TYPES,
        side=None,
        start_ts=start_ts,
        end_ts=end_ts,
        sort_by="TIMESTAMP",
        sort_direction="DESC",
        verbose=False,
    )
    candidates = recent_condition_choices(preview_rows, limit=500)
    if not candidates:
        return None

    processed = {normalize_condition_id(item) for item in processed_condition_ids}
    for candidate in candidates:
        condition_id = normalize_condition_id(candidate.get("condition_id"))
        if not condition_id or condition_id in processed:
            continue

        market = fetch_market_by_condition_id(session, condition_id, timeout)
        if market is None:
            continue
        if not market_is_active(market):
            continue

        return {
            "condition_id": condition_id,
            "title": market_title_from_metadata(market, str(candidate.get("title") or "")),
            "latest_ts": int(candidate.get("latest_ts") or -1),
            "row_count": int(candidate.get("row_count") or 0),
            "market": market,
        }

    return None


def collect_market_rows_until_inactive(
    session: requests.Session,
    wallet: str,
    condition_id: str,
    initial_market: Optional[Dict[str, Any]],
    initial_title: str,
    timeout: int,
    page_limit: int,
    poll_seconds: int,
    finalize_grace_seconds: int,
    start_ts: Optional[int],
    end_ts: Optional[int],
    max_pages: Optional[int],
) -> tuple[List[Dict[str, Any]], str, Optional[Dict[str, Any]]]:
    condition_key = normalize_condition_id(condition_id)
    if not condition_key:
        raise RuntimeError("Missing condition ID for continuous collection.")

    title = market_title_from_metadata(initial_market, initial_title)
    effective_start_ts = start_ts
    market_start_ts = market_start_ts_from_metadata(initial_market)
    if market_start_ts is not None and (effective_start_ts is None or market_start_ts > effective_start_ts):
        effective_start_ts = market_start_ts

    rows_for_market: List[Dict[str, Any]] = []
    last_row_count = -1
    inactive_since: Optional[float] = None
    last_known_active = market_is_active(initial_market)
    latest_market = initial_market

    while True:
        try:
            fetched = fetch_activity(
                session=session,
                wallet=wallet,
                page_limit=page_limit,
                timeout=timeout,
                max_pages=max_pages,
                types=CONTINUOUS_ACTIVITY_TYPES,
                side=None,
                start_ts=effective_start_ts,
                end_ts=end_ts,
                sort_by="TIMESTAMP",
                sort_direction="DESC",
                verbose=False,
            )
        except requests.RequestException as exc:
            print(
                f"Warning: activity fetch failed while tracking {title}: {exc}. Retrying in {poll_seconds}s.",
                file=sys.stderr,
            )
            time.sleep(poll_seconds)
            continue

        filtered_rows = filter_rows_by_condition_id(fetched, condition_key)
        rows_for_market = deduplicate_activity_rows(filtered_rows)
        if len(rows_for_market) != last_row_count:
            latest_ts = -1
            for row in rows_for_market:
                try:
                    ts_value = int(row.get("timestamp"))
                except (TypeError, ValueError):
                    continue
                latest_ts = max(latest_ts, ts_value)
            latest_text = unix_to_iso(latest_ts) if latest_ts >= 0 else ""
            print(
                (
                    f"Tracking {title}: captured {len(rows_for_market)} rows"
                    + (f", latest {latest_text}" if latest_text else "")
                ),
                file=sys.stderr,
            )
            last_row_count = len(rows_for_market)

        status_fetch_failed = False
        market = None
        try:
            market = fetch_market_by_condition_id(session, condition_key, timeout)
        except requests.RequestException as exc:
            status_fetch_failed = True
            print(
                f"Warning: market status refresh failed for {title}: {exc}. Keeping previous status.",
                file=sys.stderr,
            )

        if market is not None:
            latest_market = market
            title = market_title_from_metadata(market, title)
            is_active_now = market_is_active(market)
            last_known_active = is_active_now
        elif status_fetch_failed:
            is_active_now = last_known_active
        else:
            is_active_now = False

        if is_active_now:
            if inactive_since is not None:
                print(f"{title} became active again; resuming collection.", file=sys.stderr)
            inactive_since = None
            time.sleep(poll_seconds)
            continue

        if inactive_since is None:
            inactive_since = time.time()
            print(
                (
                    f"{title} is no longer active. Waiting {finalize_grace_seconds}s "
                    "before final export."
                ),
                file=sys.stderr,
            )
            time.sleep(poll_seconds)
            continue

        elapsed = time.time() - inactive_since
        if elapsed < finalize_grace_seconds:
            time.sleep(poll_seconds)
            continue

        try:
            final_fetch = fetch_activity(
                session=session,
                wallet=wallet,
                page_limit=page_limit,
                timeout=timeout,
                max_pages=max_pages,
                types=CONTINUOUS_ACTIVITY_TYPES,
                side=None,
                start_ts=effective_start_ts,
                end_ts=end_ts,
                sort_by="TIMESTAMP",
                sort_direction="DESC",
                verbose=False,
            )
            rows_for_market = deduplicate_activity_rows(
                filter_rows_by_condition_id(final_fetch, condition_key)
            )
        except requests.RequestException as exc:
            print(
                f"Warning: final backfill fetch failed for {title}: {exc}. Exporting collected rows.",
                file=sys.stderr,
            )

        return (rows_for_market, title, latest_market)


def run_continuous_collection(
    session: requests.Session,
    wallet: str,
    output_base_path: str,
    timeout: int,
    page_limit: int,
    poll_seconds: int,
    finalize_grace_seconds: int,
    discovery_pages: int,
    max_markets: Optional[int],
    state_path: str,
    analysis_enabled: bool,
    scenario_min_bets: List[float],
    scenario_max_bets: List[float],
    scenario_max_prices: List[float],
    scenario_auto_min_bets: bool,
    scenario_auto_max_bets: bool,
    scenario_auto_max_prices: bool,
    start_ts: Optional[int],
    end_ts: Optional[int],
    max_pages: Optional[int],
    telegram_bot_token: Optional[str],
    telegram_chat_id: Optional[str],
    telegram_batch_size: int,
    telegram_send_existing: bool,
) -> int:
    state = load_continuous_state(state_path)
    raw_ids = state.get("processed_condition_ids")
    if not isinstance(raw_ids, list):
        raw_ids = []
    processed_ids = [normalize_condition_id(item) for item in raw_ids if normalize_condition_id(item)]
    processed_set = set(processed_ids)
    telegram_enabled = bool(telegram_bot_token and telegram_chat_id)

    if telegram_enabled:
        ensure_telegram_state(
            state,
            batch_size=telegram_batch_size,
            send_existing=telegram_send_existing,
        )
        save_continuous_state(state_path, state)

    print(
        (
            "Continuous mode started. "
            f"Processed markets in state: {len(processed_set)}. "
            f"Polling every {poll_seconds}s."
        ),
        file=sys.stderr,
    )
    print(f"Continuous state file: {state_path}", file=sys.stderr)
    if telegram_enabled:
        print(
            (
                "Telegram delivery enabled. "
                f"Batch size: {telegram_batch_size} markets per ZIP."
            ),
            file=sys.stderr,
        )
        if telegram_send_existing:
            print("Telegram backlog mode: enabled (will send existing unsent exports).", file=sys.stderr)

        flush_telegram_batches(
            state=state,
            state_path=state_path,
            bot_token=str(telegram_bot_token),
            chat_id=str(telegram_chat_id),
            batch_size=telegram_batch_size,
            timeout=max(timeout, TELEGRAM_DEFAULT_SEND_TIMEOUT_SECONDS),
        )
    else:
        print("Telegram delivery disabled (missing token/chat id).", file=sys.stderr)

    exported_in_run = 0
    try:
        while True:
            if max_markets is not None and exported_in_run >= max_markets:
                print(
                    f"Reached --continuous-max-markets={max_markets}. Stopping.",
                    file=sys.stderr,
                )
                return 0

            try:
                candidate = choose_next_active_market(
                    session=session,
                    wallet=wallet,
                    timeout=timeout,
                    page_limit=page_limit,
                    discovery_pages=discovery_pages,
                    processed_condition_ids=processed_set,
                    start_ts=start_ts,
                    end_ts=end_ts,
                )
            except requests.RequestException as exc:
                print(
                    f"Warning: failed to discover next active market: {exc}. Retrying in {poll_seconds}s.",
                    file=sys.stderr,
                )
                time.sleep(poll_seconds)
                continue

            if candidate is None:
                print(
                    (
                        "No unprocessed active market found in recent wallet activity. "
                        f"Retrying in {poll_seconds}s."
                    ),
                    file=sys.stderr,
                )
                time.sleep(poll_seconds)
                continue

            condition_id = normalize_condition_id(candidate.get("condition_id"))
            if not condition_id:
                time.sleep(poll_seconds)
                continue

            market_title = str(candidate.get("title") or "").strip() or "(untitled market)"
            print(
                (
                    f"Tracking next active market: {market_title} "
                    f"(conditionId {condition_id})"
                ),
                file=sys.stderr,
            )

            rows, final_title, final_market = collect_market_rows_until_inactive(
                session=session,
                wallet=wallet,
                condition_id=condition_id,
                initial_market=candidate.get("market") if isinstance(candidate.get("market"), dict) else None,
                initial_title=market_title,
                timeout=timeout,
                page_limit=page_limit,
                poll_seconds=poll_seconds,
                finalize_grace_seconds=finalize_grace_seconds,
                start_ts=start_ts,
                end_ts=end_ts,
                max_pages=max_pages,
            )

            selected_titles = [final_title]
            output_path = output_path_with_market_label(output_base_path, selected_titles)
            count = write_csv(rows, output_path)

            analysis_result: Optional[Dict[str, Any]] = None
            if analysis_enabled:
                analysis_result = generate_analysis_files(
                    rows=rows,
                    wallet=wallet,
                    selected_market_titles=selected_titles,
                    output_csv_path=output_path,
                    scenario_min_bets=scenario_min_bets,
                    scenario_max_bets=scenario_max_bets,
                    scenario_max_prices=scenario_max_prices,
                    scenario_auto_min_bets=scenario_auto_min_bets,
                    scenario_auto_max_bets=scenario_auto_max_bets,
                    scenario_auto_max_prices=scenario_auto_max_prices,
                )

            print(f"Wrote {count} rows to {output_path}")
            if analysis_result is not None:
                print(f"Wrote analysis report to {analysis_result['analysis_path']}")
                print(f"Wrote scenario table to {analysis_result['scenarios_path']}")

            processed_set.add(condition_id)
            state["processed_condition_ids"] = sorted(processed_set)

            exports = state.get("exports")
            if not isinstance(exports, list):
                exports = []
            exports.append(
                {
                    "condition_id": condition_id,
                    "market_id": str(final_market.get("id") or "") if isinstance(final_market, dict) else "",
                    "title": final_title,
                    "csv_path": output_path,
                    "analysis_path": analysis_result["analysis_path"] if analysis_result is not None else "",
                    "scenarios_path": analysis_result["scenarios_path"] if analysis_result is not None else "",
                    "exported_utc": datetime.now(timezone.utc).isoformat(),
                }
            )
            state["exports"] = exports
            save_continuous_state(state_path, state)

            if telegram_enabled:
                flush_telegram_batches(
                    state=state,
                    state_path=state_path,
                    bot_token=str(telegram_bot_token),
                    chat_id=str(telegram_chat_id),
                    batch_size=telegram_batch_size,
                    timeout=max(timeout, TELEGRAM_DEFAULT_SEND_TIMEOUT_SECONDS),
                )

            exported_in_run += 1
    except KeyboardInterrupt:
        print("Continuous mode interrupted by user.", file=sys.stderr)
        return 0


def unix_to_iso(ts: Any) -> str:
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
    except Exception:
        return ""


def parse_number(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        text = str(value).strip()
        if not text:
            return 0.0
        return float(text)
    except (TypeError, ValueError):
        return 0.0


def safe_div(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def expand_number_token(token: str, arg_name: str) -> List[float]:
    raw = token.strip()
    if not raw:
        return []

    if ":" not in raw:
        try:
            return [float(raw)]
        except ValueError as exc:
            raise ValueError(f"Invalid number in --{arg_name}: {raw!r}") from exc

    parts = [part.strip() for part in raw.split(":")]
    if len(parts) not in {2, 3} or any(part == "" for part in parts):
        raise ValueError(
            f"Invalid range in --{arg_name}: {raw!r}. Use start:end or start:end:step"
        )

    try:
        start = float(parts[0])
        end = float(parts[1])
        if len(parts) == 3:
            step = float(parts[2])
        else:
            step = 1.0 if end >= start else -1.0
    except ValueError as exc:
        raise ValueError(f"Invalid range values in --{arg_name}: {raw!r}") from exc

    if step == 0:
        raise ValueError(f"Range step in --{arg_name} cannot be 0: {raw!r}")

    direction = end - start
    if direction == 0:
        return [start]
    if direction > 0 and step < 0:
        raise ValueError(f"Range step in --{arg_name} must be positive: {raw!r}")
    if direction < 0 and step > 0:
        raise ValueError(f"Range step in --{arg_name} must be negative: {raw!r}")

    values: List[float] = []
    current = start
    epsilon = abs(step) * 1e-12 + 1e-12
    limit = 0
    max_points = 10000
    if step > 0:
        while current <= end + epsilon:
            values.append(round(current, 10))
            current += step
            limit += 1
            if limit > max_points:
                raise ValueError(
                    f"Range in --{arg_name} produced too many values ({max_points}+): {raw!r}"
                )
    else:
        while current >= end - epsilon:
            values.append(round(current, 10))
            current += step
            limit += 1
            if limit > max_points:
                raise ValueError(
                    f"Range in --{arg_name} produced too many values ({max_points}+): {raw!r}"
                )

    return values


def parse_number_list_arg(
    raw: str,
    arg_name: str,
    minimum: Optional[float] = None,
    maximum: Optional[float] = None,
) -> List[float]:
    values: List[float] = []
    for part in str(raw or "").split(","):
        token = part.strip()
        if not token:
            continue
        expanded = expand_number_token(token, arg_name)
        for value in expanded:
            if math.isnan(value) or math.isinf(value):
                raise ValueError(f"Invalid number in --{arg_name}: {token!r}")
            if minimum is not None and value < minimum:
                raise ValueError(f"Values in --{arg_name} must be >= {minimum}")
            if maximum is not None and value > maximum:
                raise ValueError(f"Values in --{arg_name} must be <= {maximum}")
            values.append(value)

    if not values:
        raise ValueError(f"--{arg_name} must include at least one number")

    return sorted(set(values))


def parse_threshold_input(
    raw: str,
    arg_name: str,
    minimum: Optional[float] = None,
    maximum: Optional[float] = None,
    allow_auto: bool = True,
) -> tuple[List[float], bool]:
    text = str(raw or "").strip().casefold()
    if allow_auto and text in {"auto", "all"}:
        return ([], True)

    values = parse_number_list_arg(raw, arg_name, minimum=minimum, maximum=maximum)
    return (values, False)


def parse_max_prices_input(raw: str) -> tuple[List[float], bool]:
    return parse_threshold_input(
        raw,
        arg_name="scenario-max-prices",
        minimum=0.0,
        maximum=1.0,
    )


def parse_min_bets_input(raw: str) -> tuple[List[float], bool]:
    return parse_threshold_input(
        raw,
        arg_name="scenario-min-bets",
        minimum=0.0,
        allow_auto=True,
    )


def parse_max_bets_input(raw: str) -> tuple[List[float], bool]:
    return parse_threshold_input(
        raw,
        arg_name="scenario-max-bets",
        minimum=0.0,
        allow_auto=True,
    )


def cli_flag_was_provided(flag: str) -> bool:
    for token in sys.argv[1:]:
        if token == flag or token.startswith(flag + "="):
            return True
    return False


def prompt_list_with_default(
    prompt: str,
    defaults: List[float],
    arg_name_for_validation: str,
    minimum: Optional[float] = None,
    maximum: Optional[float] = None,
) -> List[float]:
    while True:
        default_text = ",".join(str(v) for v in defaults)
        try:
            raw = input(f"{prompt} [{default_text}]: ").strip()
        except EOFError:
            return defaults

        if not raw:
            return defaults
        try:
            return parse_number_list_arg(raw, arg_name_for_validation, minimum=minimum, maximum=maximum)
        except ValueError as exc:
            print(f"Invalid input: {exc}", file=sys.stderr)


def prompt_max_prices_with_default(
    default_input: str = DEFAULT_SCENARIO_MAX_PRICES_INPUT,
) -> tuple[List[float], bool]:
    while True:
        try:
            raw = input(
                "Enter max price values (0-1, comma-separated/ranges, or 'auto') "
                f"[{default_input}]: "
            ).strip()
        except EOFError:
            raw = ""

        candidate = raw if raw else default_input
        try:
            return parse_max_prices_input(candidate)
        except ValueError as exc:
            print(f"Invalid input: {exc}", file=sys.stderr)


def prompt_analysis_setup_interactively(
    default_min_bets: List[float],
    default_max_bets: List[float],
    default_max_prices_input: str,
) -> tuple[bool, List[float], List[float], List[float], bool, bool, bool]:
    print("", file=sys.stderr)
    print("Analysis setup:", file=sys.stderr)
    print("  1) Auto-search best settings (recommended)", file=sys.stderr)
    print("  2) Custom min/max bet + max price grid", file=sys.stderr)
    print("  0) Skip analysis files", file=sys.stderr)

    while True:
        try:
            choice = input("Choose analysis mode [1]: ").strip()
        except EOFError:
            choice = ""

        if choice in {"", "1"}:
            return (True, [], [], [], True, True, True)
        if choice == "0":
            return (False, [], [], [], False, False, False)
        if choice == "2":
            min_bets = prompt_list_with_default(
                "Enter min bet values (USDC, comma-separated/ranges)",
                default_min_bets,
                "scenario-min-bets",
                minimum=0.0,
            )
            max_bets = prompt_list_with_default(
                "Enter max bet values (USDC, comma-separated/ranges)",
                default_max_bets,
                "scenario-max-bets",
                minimum=0.0,
            )
            max_prices, auto_max_prices = prompt_max_prices_with_default(default_max_prices_input)
            return (True, min_bets, max_bets, max_prices, False, False, auto_max_prices)

        print("Please enter 0, 1, or 2.", file=sys.stderr)


def row_signature(row: Dict[str, Any]) -> tuple[str, ...]:
    return (
        str(row.get("timestamp") or ""),
        str(row.get("type") or ""),
        str(row.get("side") or "").upper(),
        str(row.get("usdcSize") or ""),
        str(row.get("size") or ""),
        str(row.get("price") or ""),
        str(row.get("outcome") or ""),
        str(row.get("outcomeIndex") or ""),
        normalize_for_match(row.get("title")),
        str(row.get("transactionHash") or ""),
        str(row.get("asset") or ""),
        str(row.get("conditionId") or ""),
    )


def deduplicate_activity_rows(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    unique_rows: List[Dict[str, Any]] = []
    seen: set[tuple[str, ...]] = set()
    for row in rows:
        sig = row_signature(row)
        if sig in seen:
            continue
        seen.add(sig)
        unique_rows.append(row)
    return unique_rows


def compute_leg_stats(trades: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    legs: Dict[str, Dict[str, float]] = {}
    for row in trades:
        outcome = str(row.get("outcome") or "Unknown")
        side = str(row.get("side") or "").upper()
        usdc = parse_number(row.get("usdcSize"))
        size = parse_number(row.get("size"))

        leg = legs.setdefault(
            outcome,
            {
                "buy_count": 0.0,
                "buy_usdc": 0.0,
                "buy_size": 0.0,
                "sell_count": 0.0,
                "sell_usdc": 0.0,
                "sell_size": 0.0,
                "avg_buy_price": 0.0,
                "avg_sell_price": 0.0,
                "net_size": 0.0,
                "redeem_usdc": 0.0,
                "net_pnl": 0.0,
            },
        )

        if side == "BUY":
            leg["buy_count"] += 1.0
            leg["buy_usdc"] += usdc
            leg["buy_size"] += size
        elif side == "SELL":
            leg["sell_count"] += 1.0
            leg["sell_usdc"] += usdc
            leg["sell_size"] += size

    for leg in legs.values():
        leg["avg_buy_price"] = safe_div(leg["buy_usdc"], leg["buy_size"])
        leg["avg_sell_price"] = safe_div(leg["sell_usdc"], leg["sell_size"])
        leg["net_size"] = leg["buy_size"] - leg["sell_size"]

    return legs


def infer_winning_outcome(legs: Dict[str, Dict[str, float]], redeem_total: float) -> Optional[str]:
    if redeem_total <= 0:
        return None

    candidates: List[tuple[float, float, str]] = []
    for outcome, leg in legs.items():
        net_size = leg.get("net_size", 0.0)
        if net_size <= 0:
            continue
        candidates.append((abs(net_size - redeem_total), -net_size, outcome))

    if not candidates:
        return None

    candidates.sort()
    return candidates[0][2]


def make_analysis_paths(output_csv_path: str) -> tuple[str, str]:
    output_path = Path(output_csv_path)
    if output_path.suffix.lower() == ".csv":
        stem = output_path.with_suffix("")
    else:
        stem = output_path
    return (str(stem) + "_analysis.md", str(stem) + "_scenarios.csv")


def build_copy_scenarios(
    trades: Iterable[Dict[str, Any]],
    winning_outcome: Optional[str],
    min_bets: List[float],
    max_bets: List[float],
    max_prices: List[float],
    forced_points: Optional[List[tuple[float, float, float]]] = None,
) -> List[Dict[str, Any]]:
    buy_data: List[tuple[float, float, float, str]] = []
    for row in trades:
        if str(row.get("side") or "").upper() != "BUY":
            continue
        buy_data.append(
            (
                parse_number(row.get("usdcSize")),
                parse_number(row.get("price")),
                parse_number(row.get("size")),
                str(row.get("outcome") or ""),
            )
        )

    total_buy_trades = len(buy_data)
    total_buy_spend = sum(usdc for usdc, _, _, _ in buy_data)
    scenarios: List[Dict[str, Any]] = []
    scenario_points: List[tuple[float, float, float]] = []
    seen_points: set[tuple[float, float, float]] = set()

    def add_point(min_bet: float, max_bet: float, max_price: float) -> None:
        key = (round(min_bet, 10), round(max_bet, 10), round(max_price, 10))
        if key in seen_points:
            return
        seen_points.add(key)
        scenario_points.append((min_bet, max_bet, max_price))

    for min_bet in min_bets:
        for max_bet in max_bets:
            if max_bet < min_bet:
                continue
            for max_price in max_prices:
                add_point(min_bet, max_bet, max_price)

    for point in forced_points or []:
        min_bet, max_bet, max_price = point
        if max_bet < min_bet:
            continue
        add_point(min_bet, max_bet, max_price)

    for min_bet, max_bet, max_price in sorted(scenario_points):
        kept_count = 0
        spend = 0.0
        total_size = 0.0
        winning_payout_acc = 0.0

        for usdc, price, size, outcome in buy_data:
            if usdc < min_bet or usdc > max_bet:
                continue
            if price > max_price:
                continue
            kept_count += 1
            spend += usdc
            total_size += size
            if winning_outcome is not None and outcome == winning_outcome:
                winning_payout_acc += size

        winning_payout: Optional[float] = None
        net_pnl: Optional[float] = None
        roi_pct: Optional[float] = None
        if winning_outcome is not None:
            winning_payout = winning_payout_acc
            net_pnl = winning_payout - spend
            if spend > 0:
                roi_pct = safe_div(net_pnl, spend) * 100.0
            else:
                roi_pct = 0.0

        scenarios.append(
            {
                "min_bet_usdc": min_bet,
                "max_bet_usdc": max_bet,
                "max_price": max_price,
                "kept_trades": kept_count,
                "kept_trades_pct": safe_div(kept_count, total_buy_trades) * 100.0,
                "spend_usdc": spend,
                "coverage_pct": safe_div(spend, total_buy_spend) * 100.0,
                "avg_price": safe_div(spend, total_size),
                "winning_payout": winning_payout,
                "net_pnl": net_pnl,
                "roi_pct": roi_pct,
            }
        )

    scenarios.sort(key=lambda row: (row["min_bet_usdc"], row["max_bet_usdc"], row["max_price"]))
    return scenarios


def choose_best_scenarios(
    scenarios: List[Dict[str, Any]],
    baseline_spend: float,
    winner_known: bool,
) -> Dict[str, Dict[str, Any]]:
    if not scenarios:
        return {}

    with_spend = [row for row in scenarios if row["spend_usdc"] > 0]
    if not with_spend:
        return {}

    if not winner_known:
        widest = max(with_spend, key=lambda row: row["coverage_pct"])
        return {"largest_coverage": widest}

    pnl_rows = [row for row in with_spend if row["net_pnl"] is not None and row["roi_pct"] is not None]
    if not pnl_rows:
        return {}

    best_net = max(pnl_rows, key=lambda row: (row["net_pnl"], row["roi_pct"], row["spend_usdc"]))

    roi_floor = baseline_spend * 0.15
    roi_rows = [row for row in pnl_rows if row["spend_usdc"] >= roi_floor]
    if not roi_rows:
        roi_rows = pnl_rows
    best_roi = max(roi_rows, key=lambda row: (row["roi_pct"], row["net_pnl"], row["spend_usdc"]))

    balanced_rows = [row for row in pnl_rows if row["coverage_pct"] >= 50.0]
    if not balanced_rows:
        balanced_rows = pnl_rows
    best_balanced = max(
        balanced_rows,
        key=lambda row: (row["net_pnl"], row["coverage_pct"], row["roi_pct"]),
    )

    return {
        "best_net_pnl": best_net,
        "best_roi": best_roi,
        "best_balanced": best_balanced,
    }


def top_scenarios_by_metric(
    scenarios: Iterable[Dict[str, Any]],
    metric: str,
    limit: int,
    min_spend: float = 0.0,
) -> List[Dict[str, Any]]:
    scored = [
        row
        for row in scenarios
        if row.get(metric) is not None and row.get("spend_usdc", 0.0) >= min_spend
    ]
    if not scored and min_spend > 0:
        scored = [row for row in scenarios if row.get(metric) is not None and row.get("spend_usdc", 0.0) > 0]
    if not scored:
        return []

    scored.sort(
        key=lambda row: (
            row.get(metric, float("-inf")),
            row.get("net_pnl", float("-inf")) if row.get("net_pnl") is not None else float("-inf"),
            row.get("coverage_pct", 0.0),
            -row.get("max_price", 0.0),
        ),
        reverse=True,
    )
    return scored[: max(1, limit)]


def market_title_for_row(row: Dict[str, Any]) -> str:
    raw = str(row.get("title") or "").strip()
    return raw if raw else "(untitled market)"


def group_rows_by_market(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    groups: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        title = market_title_for_row(row)
        key = normalize_for_match(title)
        group = groups.setdefault(
            key,
            {
                "title": title,
                "rows": [],
                "latest_ts": -1,
            },
        )
        group["rows"].append(row)
        try:
            ts_value = int(row.get("timestamp"))
        except (TypeError, ValueError):
            ts_value = -1
        if ts_value > group["latest_ts"]:
            group["latest_ts"] = ts_value

    grouped = list(groups.values())
    grouped.sort(key=lambda item: (-item["latest_ts"], str(item["title"])))
    return grouped


def auto_grid_level_limit(buy_count: int) -> int:
    if buy_count > 2000:
        return AUTO_GRID_MAX_LEVELS_HIGH
    if buy_count > 800:
        return AUTO_GRID_MAX_LEVELS_MID
    return AUTO_GRID_MAX_LEVELS_DEFAULT


def downsample_sorted_levels(values: List[float], max_count: int) -> List[float]:
    unique_sorted = sorted(set(values))
    if len(unique_sorted) <= max_count:
        return unique_sorted
    if max_count <= 1:
        return [unique_sorted[0]]

    last_idx = len(unique_sorted) - 1
    indices = {
        int(round(i * last_idx / (max_count - 1)))
        for i in range(max_count)
    }
    return [unique_sorted[idx] for idx in sorted(indices)]


def resolve_min_bets_for_subset(
    buy_rows: List[Dict[str, Any]],
    configured_min_bets: List[float],
    auto_mode: bool,
) -> List[float]:
    if not auto_mode:
        return configured_min_bets

    levels = [0.0]
    for row in buy_rows:
        usdc = parse_number(row.get("usdcSize"))
        if usdc >= 0:
            levels.append(round(usdc, 2))
    limit = auto_grid_level_limit(len(buy_rows))
    return downsample_sorted_levels(levels, limit)


def resolve_max_bets_for_subset(
    buy_rows: List[Dict[str, Any]],
    configured_max_bets: List[float],
    auto_mode: bool,
) -> List[float]:
    if not auto_mode:
        return configured_max_bets

    levels: List[float] = []
    for row in buy_rows:
        usdc = parse_number(row.get("usdcSize"))
        if usdc >= 0:
            levels.append(round(usdc, 2))
    if not levels:
        levels = [0.0]
    limit = auto_grid_level_limit(len(buy_rows))
    return downsample_sorted_levels(levels, limit)


def resolve_max_prices_for_subset(
    buy_rows: List[Dict[str, Any]],
    configured_max_prices: List[float],
    auto_mode: bool,
) -> List[float]:
    if not auto_mode:
        return configured_max_prices

    observed_set: set[float] = set()
    for row in buy_rows:
        price = parse_number(row.get("price"))
        if 0.0 <= price <= 1.0:
            observed_set.add(round(price, 4))
    observed = sorted(observed_set)
    if not observed:
        return [1.0]
    if observed[-1] < 1.0:
        observed.append(1.0)
    limit = auto_grid_level_limit(len(buy_rows))
    return downsample_sorted_levels(observed, limit)


def analyze_row_subset(
    rows: List[Dict[str, Any]],
    scenario_min_bets: List[float],
    scenario_max_bets: List[float],
    scenario_max_prices: List[float],
    scenario_auto_min_bets: bool,
    scenario_auto_max_bets: bool,
    scenario_auto_max_prices: bool,
) -> Dict[str, Any]:
    trade_rows = [row for row in rows if str(row.get("type") or "").upper() == "TRADE"]
    redeem_rows = [row for row in rows if str(row.get("type") or "").upper() == "REDEEM"]

    buy_rows = [row for row in trade_rows if str(row.get("side") or "").upper() == "BUY"]
    sell_rows = [row for row in trade_rows if str(row.get("side") or "").upper() == "SELL"]

    total_buy_spend = sum(parse_number(row.get("usdcSize")) for row in buy_rows)
    total_buy_size = sum(parse_number(row.get("size")) for row in buy_rows)
    total_sell_proceeds = sum(parse_number(row.get("usdcSize")) for row in sell_rows)
    total_sell_size = sum(parse_number(row.get("size")) for row in sell_rows)
    total_redeem = sum(parse_number(row.get("usdcSize")) for row in redeem_rows)

    net_pnl = total_sell_proceeds + total_redeem - total_buy_spend
    roi_pct = safe_div(net_pnl, total_buy_spend) * 100.0 if total_buy_spend > 0 else 0.0

    leg_stats = compute_leg_stats(trade_rows)
    winning_outcome = infer_winning_outcome(leg_stats, total_redeem)
    if winning_outcome and winning_outcome in leg_stats:
        leg_stats[winning_outcome]["redeem_usdc"] = total_redeem
    for leg in leg_stats.values():
        leg["net_pnl"] = leg["sell_usdc"] + leg["redeem_usdc"] - leg["buy_usdc"]

    min_bets_for_subset = resolve_min_bets_for_subset(
        buy_rows=buy_rows,
        configured_min_bets=scenario_min_bets,
        auto_mode=scenario_auto_min_bets,
    )
    max_bets_for_subset = resolve_max_bets_for_subset(
        buy_rows=buy_rows,
        configured_max_bets=scenario_max_bets,
        auto_mode=scenario_auto_max_bets,
    )

    max_prices_for_subset = resolve_max_prices_for_subset(
        buy_rows=buy_rows,
        configured_max_prices=scenario_max_prices,
        auto_mode=scenario_auto_max_prices,
    )

    forced_points: List[tuple[float, float, float]] = [
        (REFERENCE_MIN_BET_USDC, REFERENCE_MAX_BET_USDC, REFERENCE_MAX_PRICE),
        (REFERENCE_2_MIN_BET_USDC, REFERENCE_2_MAX_BET_USDC, REFERENCE_2_MAX_PRICE),
        (REFERENCE_3_MIN_BET_USDC, REFERENCE_3_MAX_BET_USDC, REFERENCE_3_MAX_PRICE),
    ]

    scenarios = build_copy_scenarios(
        trades=trade_rows,
        winning_outcome=winning_outcome,
        min_bets=min_bets_for_subset,
        max_bets=max_bets_for_subset,
        max_prices=max_prices_for_subset,
        forced_points=forced_points,
    )
    winner_known = winning_outcome is not None
    best_scenarios = choose_best_scenarios(scenarios, total_buy_spend, winner_known)
    reference_scenario = find_scenario(
        scenarios,
        min_bet=REFERENCE_MIN_BET_USDC,
        max_bet=REFERENCE_MAX_BET_USDC,
        max_price=REFERENCE_MAX_PRICE,
    )
    reference_scenario_2 = find_scenario(
        scenarios,
        min_bet=REFERENCE_2_MIN_BET_USDC,
        max_bet=REFERENCE_2_MAX_BET_USDC,
        max_price=REFERENCE_2_MAX_PRICE,
    )
    reference_scenario_3 = find_scenario(
        scenarios,
        min_bet=REFERENCE_3_MIN_BET_USDC,
        max_bet=REFERENCE_3_MAX_BET_USDC,
        max_price=REFERENCE_3_MAX_PRICE,
    )

    min_spend_for_rank = total_buy_spend * 0.05
    top_by_pnl = top_scenarios_by_metric(
        scenarios,
        metric="net_pnl",
        limit=5,
        min_spend=min_spend_for_rank,
    )
    top_by_roi = top_scenarios_by_metric(
        scenarios,
        metric="roi_pct",
        limit=5,
        min_spend=min_spend_for_rank,
    )

    return {
        "rows": rows,
        "row_count": len(rows),
        "trade_count": len(trade_rows),
        "redeem_count": len(redeem_rows),
        "total_buy_spend": total_buy_spend,
        "total_buy_size": total_buy_size,
        "total_sell_proceeds": total_sell_proceeds,
        "total_sell_size": total_sell_size,
        "total_redeem": total_redeem,
        "net_pnl": net_pnl,
        "roi_pct": roi_pct,
        "winning_outcome": winning_outcome,
        "winner_known": winner_known,
        "leg_stats": leg_stats,
        "scenarios": scenarios,
        "min_bets_tested_count": len(min_bets_for_subset),
        "max_bets_tested_count": len(max_bets_for_subset),
        "max_prices_tested_count": len(max_prices_for_subset),
        "auto_min_bets": scenario_auto_min_bets,
        "auto_max_bets": scenario_auto_max_bets,
        "auto_max_prices": scenario_auto_max_prices,
        "reference_scenario": reference_scenario,
        "reference_scenario_2": reference_scenario_2,
        "reference_scenario_3": reference_scenario_3,
        "best_scenarios": best_scenarios,
        "top_by_pnl": top_by_pnl,
        "top_by_roi": top_by_roi,
    }


def find_scenario(
    scenarios: Iterable[Dict[str, Any]],
    min_bet: float,
    max_bet: float,
    max_price: float,
) -> Optional[Dict[str, Any]]:
    for row in scenarios:
        if (
            abs(row["min_bet_usdc"] - min_bet) < 1e-9
            and abs(row["max_bet_usdc"] - max_bet) < 1e-9
            and abs(row["max_price"] - max_price) < 1e-9
        ):
            return row
    return None


def write_scenarios_csv(
    scenarios: Iterable[Dict[str, Any]],
    output_path: str,
) -> None:
    fieldnames = [
        "min_bet_usdc",
        "max_bet_usdc",
        "max_price",
        "kept_trades",
        "kept_trades_pct",
        "spend_usdc",
        "coverage_pct",
        "avg_price",
        "winning_payout",
        "net_pnl",
        "roi_pct",
    ]
    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in scenarios:
            out_row = dict(row)
            for key in ["winning_payout", "net_pnl", "roi_pct"]:
                if out_row.get(key) is None:
                    out_row[key] = ""
            writer.writerow(out_row)


def scenario_to_text(row: Dict[str, Any], winner_known: bool) -> str:
    base = (
        f"min ${row['min_bet_usdc']:.2f}, max ${row['max_bet_usdc']:.2f}, "
        f"max price {row['max_price']:.2f}, kept {int(row['kept_trades'])} trades, "
        f"spend {row['spend_usdc']:.2f} USDC"
    )
    if winner_known and row.get("net_pnl") is not None and row.get("roi_pct") is not None:
        return base + f", PnL {row['net_pnl']:.2f} USDC, ROI {row['roi_pct']:.2f}%"
    return base


def append_scenario_table(
    lines: List[str],
    scenarios: List[Dict[str, Any]],
    winner_known: bool,
) -> None:
    if not scenarios:
        lines.append("- No scenarios available for this scope.")
        lines.append("")
        return

    lines.append(
        "| Min Bet | Max Bet | Max Price | Kept Trades | Spend | Coverage | Avg Price | Net PnL | ROI |"
    )
    lines.append("|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for row in scenarios:
        pnl_text = "n/a"
        roi_text = "n/a"
        if winner_known and row.get("net_pnl") is not None and row.get("roi_pct") is not None:
            pnl_text = f"{row['net_pnl']:.2f}"
            roi_text = f"{row['roi_pct']:.2f}%"
        lines.append(
            f"| {row['min_bet_usdc']:.2f} | {row['max_bet_usdc']:.2f} | {row['max_price']:.2f}"
            f" | {int(row['kept_trades'])} | {row['spend_usdc']:.2f} | {row['coverage_pct']:.2f}%"
            f" | {row['avg_price']:.4f} | {pnl_text} | {roi_text} |"
        )
    lines.append("")


def append_named_reference_table(
    lines: List[str],
    named_scenarios: List[tuple[str, Optional[Dict[str, Any]]]],
    winner_known: bool,
) -> None:
    available = [(name, row) for name, row in named_scenarios if row is not None]
    if not available:
        lines.append("- No reference points available for this scope.")
        lines.append("")
        return

    lines.append(
        "| Reference | Min Bet | Max Bet | Max Price | Kept Trades | Spend | Coverage | Avg Price | Net PnL | ROI |"
    )
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for name, row in available:
        assert row is not None
        pnl_text = "n/a"
        roi_text = "n/a"
        if winner_known and row.get("net_pnl") is not None and row.get("roi_pct") is not None:
            pnl_text = f"{row['net_pnl']:.2f}"
            roi_text = f"{row['roi_pct']:.2f}%"
        lines.append(
            f"| {name} | {row['min_bet_usdc']:.2f} | {row['max_bet_usdc']:.2f} | {row['max_price']:.2f}"
            f" | {int(row['kept_trades'])} | {row['spend_usdc']:.2f} | {row['coverage_pct']:.2f}%"
            f" | {row['avg_price']:.4f} | {pnl_text} | {roi_text} |"
        )
    lines.append("")


def append_leg_breakdown_table(lines: List[str], leg_stats: Dict[str, Dict[str, float]]) -> None:
    if not leg_stats:
        lines.append("- No trade legs available for this scope.")
        lines.append("")
        return

    lines.append(
        "| Outcome | Buy USDC | Buy Size | Avg Buy | Sell USDC | Sell Size | Avg Sell | Net Size | Redeem | Net PnL |"
    )
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    ordered_outcomes = sorted(leg_stats.keys(), key=lambda name: (name not in {"Up", "Down"}, name))
    for outcome in ordered_outcomes:
        leg = leg_stats[outcome]
        lines.append(
            "| "
            + outcome
            + f" | {leg['buy_usdc']:.6f} | {leg['buy_size']:.6f} | {leg['avg_buy_price']:.6f}"
            + f" | {leg['sell_usdc']:.6f} | {leg['sell_size']:.6f} | {leg['avg_sell_price']:.6f}"
            + f" | {leg['net_size']:.6f} | {leg['redeem_usdc']:.6f} | {leg['net_pnl']:.6f} |"
        )
    lines.append("")


def write_analysis_report(
    output_path: str,
    wallet: str,
    selected_market_titles: Optional[List[str]],
    raw_row_count: int,
    deduped_row_count: int,
    overall_metrics: Dict[str, Any],
    market_analyses: List[Dict[str, Any]],
    scenarios_path: str,
) -> None:
    cleaned_titles = clean_market_titles(selected_market_titles)
    if not cleaned_titles:
        market_value = "(all selected markets)"
    elif len(cleaned_titles) == 1:
        market_value = cleaned_titles[0]
    elif len(cleaned_titles) <= 3:
        market_value = ", ".join(cleaned_titles)
    else:
        market_value = ", ".join(cleaned_titles[:3]) + f" (+{len(cleaned_titles) - 3} more)"

    lines: List[str] = []
    lines.append("# Polymarket Activity Analysis")
    lines.append("")
    lines.append(f"- Generated UTC: {datetime.now(timezone.utc).isoformat()}")
    lines.append(f"- Wallet: `{wallet}`")
    lines.append(f"- Market scope: {market_value}")
    lines.append(
        f"- Rows: {raw_row_count} (deduplicated: {deduped_row_count}, removed duplicates: {raw_row_count - deduped_row_count})"
    )
    lines.append(
        f"- Trades: {overall_metrics['trade_count']}, Redeems: {overall_metrics['redeem_count']}"
    )

    lines.append("")
    lines.append("## Total PnL")
    lines.append("")
    lines.append(f"- Total spend (BUY): {overall_metrics['total_buy_spend']:.6f} USDC")
    lines.append(f"- Total sold (SELL): {overall_metrics['total_sell_proceeds']:.6f} USDC")
    lines.append(f"- Total redeem: {overall_metrics['total_redeem']:.6f} USDC")
    lines.append(f"- Net realized PnL: {overall_metrics['net_pnl']:.6f} USDC")
    lines.append(f"- ROI on BUY spend: {overall_metrics['roi_pct']:.4f}%")
    lines.append(
        f"- Average BUY price: {safe_div(overall_metrics['total_buy_spend'], overall_metrics['total_buy_size']):.6f}"
    )
    if overall_metrics["total_sell_size"] > 0:
        lines.append(
            f"- Average SELL price: {safe_div(overall_metrics['total_sell_proceeds'], overall_metrics['total_sell_size']):.6f}"
        )
    if overall_metrics["winning_outcome"]:
        lines.append(f"- Inferred winning side: {overall_metrics['winning_outcome']}")
    else:
        lines.append("- Inferred winning side: unknown (no redeem evidence)")

    lines.append("")
    lines.append("## Total Leg Breakdown")
    lines.append("")
    append_leg_breakdown_table(lines, overall_metrics["leg_stats"])

    lines.append("")
    lines.append("## Copy-Setting Scenarios")
    lines.append("")
    lines.append(f"- Scenario rows: {len(overall_metrics['scenarios'])}")
    lines.append(f"- Scenario CSV: `{scenarios_path}`")
    if overall_metrics.get("auto_min_bets"):
        lines.append(
            f"- Min bet grid: auto ({overall_metrics.get('min_bets_tested_count', 0)} values)"
        )
    else:
        lines.append(
            f"- Min bet grid: manual ({overall_metrics.get('min_bets_tested_count', 0)} values)"
        )
    if overall_metrics.get("auto_max_bets"):
        lines.append(
            f"- Max bet grid: auto ({overall_metrics.get('max_bets_tested_count', 0)} values)"
        )
    else:
        lines.append(
            f"- Max bet grid: manual ({overall_metrics.get('max_bets_tested_count', 0)} values)"
        )
    if overall_metrics.get("auto_max_prices"):
        lines.append(
            f"- Max price grid: auto (all observed prices, {overall_metrics.get('max_prices_tested_count', 0)} values)"
        )
    else:
        lines.append(
            f"- Max price grid: manual ({overall_metrics.get('max_prices_tested_count', 0)} values)"
        )

    lines.append("")
    lines.append("### Reference Points (total)")
    lines.append("")
    append_named_reference_table(
        lines,
        [
            ("Reference point 1 (always included)", overall_metrics.get("reference_scenario")),
            ("Reference point 2 (always included)", overall_metrics.get("reference_scenario_2")),
            ("Reference point 3 (always included)", overall_metrics.get("reference_scenario_3")),
        ],
        overall_metrics["winner_known"],
    )

    auto_best = None
    if overall_metrics["winner_known"] and "best_net_pnl" in overall_metrics["best_scenarios"]:
        auto_best = overall_metrics["best_scenarios"]["best_net_pnl"]
    elif "largest_coverage" in overall_metrics["best_scenarios"]:
        auto_best = overall_metrics["best_scenarios"]["largest_coverage"]
    if auto_best:
        lines.append(
            "- Auto-selected best setting (script): "
            + scenario_to_text(auto_best, overall_metrics["winner_known"])
        )

    if overall_metrics["winner_known"] and "best_roi" in overall_metrics["best_scenarios"]:
        lines.append(
            "- Auto-selected best ROI setting: "
            + scenario_to_text(overall_metrics["best_scenarios"]["best_roi"], True)
        )

    example = find_scenario(overall_metrics["scenarios"], min_bet=10.0, max_bet=20.0, max_price=1.0)
    if example:
        lines.append(
            "- Example (min $10, max $20): "
            + scenario_to_text(example, overall_metrics["winner_known"])
        )

    lines.append("")
    lines.append("### Top Settings by Net PnL (total)")
    lines.append("")
    append_scenario_table(lines, overall_metrics["top_by_pnl"], overall_metrics["winner_known"])

    lines.append("### Top Settings by ROI (total)")
    lines.append("")
    append_scenario_table(lines, overall_metrics["top_by_roi"], overall_metrics["winner_known"])

    if len(market_analyses) > 1:
        lines.append("## Per-Market Windows")
        lines.append("")
        for market in market_analyses:
            lines.append(f"### {market['title']}")
            lines.append("")
            lines.append(
                f"- Trades: {market['trade_count']}, Redeems: {market['redeem_count']}, Rows: {market['row_count']}"
            )
            lines.append(f"- Spend (BUY): {market['total_buy_spend']:.6f} USDC")
            lines.append(f"- Sold (SELL): {market['total_sell_proceeds']:.6f} USDC")
            lines.append(f"- Redeem: {market['total_redeem']:.6f} USDC")
            lines.append(f"- Net PnL: {market['net_pnl']:.6f} USDC")
            lines.append(f"- ROI: {market['roi_pct']:.4f}%")
            if market["winning_outcome"]:
                lines.append(f"- Inferred winner: {market['winning_outcome']}")
            else:
                lines.append("- Inferred winner: unknown")

            market_auto_best = None
            if market["winner_known"] and "best_net_pnl" in market["best_scenarios"]:
                market_auto_best = market["best_scenarios"]["best_net_pnl"]
            elif "largest_coverage" in market["best_scenarios"]:
                market_auto_best = market["best_scenarios"]["largest_coverage"]
            if market_auto_best:
                lines.append(
                    "- Auto-selected best setting: "
                    + scenario_to_text(market_auto_best, market["winner_known"])
                )

            lines.append("Reference points:")
            append_named_reference_table(
                lines,
                [
                    ("Reference point 1", market.get("reference_scenario")),
                    ("Reference point 2", market.get("reference_scenario_2")),
                    ("Reference point 3", market.get("reference_scenario_3")),
                ],
                market["winner_known"],
            )

            lines.append("Leg breakdown:")
            append_leg_breakdown_table(lines, market["leg_stats"])

            lines.append("Top settings by Net PnL:")
            append_scenario_table(lines, market["top_by_pnl"][:3], market["winner_known"])

            lines.append("Top settings by ROI:")
            append_scenario_table(lines, market["top_by_roi"][:3], market["winner_known"])

    lines.append("## Notes")
    lines.append("")
    lines.append("- Scenario engine filters BUY trades by min/max bet size and max entry price.")
    lines.append("- If winner is known, scenario PnL assumes winning shares settle at 1 USDC each.")
    lines.append("- Auto-selected best settings are computed directly from scenario results.")
    lines.append("- Use the scenario CSV to compare thresholds in Excel/Sheets.")

    with open(output_path, "w", encoding="utf-8", newline="") as fh:
        fh.write("\n".join(lines) + "\n")


def generate_analysis_files(
    rows: Iterable[Dict[str, Any]],
    wallet: str,
    selected_market_titles: Optional[List[str]],
    output_csv_path: str,
    scenario_min_bets: List[float],
    scenario_max_bets: List[float],
    scenario_max_prices: List[float],
    scenario_auto_min_bets: bool,
    scenario_auto_max_bets: bool,
    scenario_auto_max_prices: bool,
) -> Dict[str, Any]:
    raw_rows = list(rows)
    deduped_rows = deduplicate_activity_rows(raw_rows)
    overall_metrics = analyze_row_subset(
        deduped_rows,
        scenario_min_bets=scenario_min_bets,
        scenario_max_bets=scenario_max_bets,
        scenario_max_prices=scenario_max_prices,
        scenario_auto_min_bets=scenario_auto_min_bets,
        scenario_auto_max_bets=scenario_auto_max_bets,
        scenario_auto_max_prices=scenario_auto_max_prices,
    )

    market_analyses: List[Dict[str, Any]] = []
    for group in group_rows_by_market(deduped_rows):
        market_metrics = analyze_row_subset(
            group["rows"],
            scenario_min_bets=scenario_min_bets,
            scenario_max_bets=scenario_max_bets,
            scenario_max_prices=scenario_max_prices,
            scenario_auto_min_bets=scenario_auto_min_bets,
            scenario_auto_max_bets=scenario_auto_max_bets,
            scenario_auto_max_prices=scenario_auto_max_prices,
        )
        market_metrics["title"] = group["title"]
        market_metrics["latest_ts"] = group["latest_ts"]
        market_analyses.append(market_metrics)

    analysis_path, scenarios_path = make_analysis_paths(output_csv_path)
    write_scenarios_csv(overall_metrics["scenarios"], scenarios_path)
    write_analysis_report(
        output_path=analysis_path,
        wallet=wallet,
        selected_market_titles=selected_market_titles,
        raw_row_count=len(raw_rows),
        deduped_row_count=len(deduped_rows),
        overall_metrics=overall_metrics,
        market_analyses=market_analyses,
        scenarios_path=scenarios_path,
    )

    return {
        "analysis_path": analysis_path,
        "scenarios_path": scenarios_path,
        "winning_outcome": overall_metrics["winning_outcome"],
        "net_pnl": overall_metrics["net_pnl"],
        "roi_pct": overall_metrics["roi_pct"],
    }


def normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "datetime_utc": unix_to_iso(row.get("timestamp")),
        "timestamp": row.get("timestamp"),
        "type": row.get("type"),
        "side": row.get("side"),
        "usdcSize": row.get("usdcSize"),
        "size": row.get("size"),
        "price": row.get("price"),
        "outcome": row.get("outcome"),
        "outcomeIndex": row.get("outcomeIndex"),
        "title": row.get("title"),
    }


def write_csv(rows: Iterable[Dict[str, Any]], output_path: str) -> int:
    normalized_rows = [normalize_row(r) for r in rows]

    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in normalized_rows:
            writer.writerow(row)

    return len(normalized_rows)


def main() -> int:
    args = parse_args()

    try:
        start_ts = parse_utc_to_unix(args.start)
        end_ts = parse_utc_to_unix(args.end)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2

    if start_ts is not None and end_ts is not None and start_ts > end_ts:
        print("Error: --start must be <= --end", file=sys.stderr)
        return 2
    if args.recent_markets < 1:
        print("Error: --recent-markets must be >= 1", file=sys.stderr)
        return 2
    if args.poll_seconds < 1:
        print("Error: --poll-seconds must be >= 1", file=sys.stderr)
        return 2
    if args.finalize_grace_seconds < 0:
        print("Error: --finalize-grace-seconds must be >= 0", file=sys.stderr)
        return 2
    if args.continuous_discovery_pages < 1:
        print("Error: --continuous-discovery-pages must be >= 1", file=sys.stderr)
        return 2
    if args.continuous_max_markets is not None and args.continuous_max_markets < 1:
        print("Error: --continuous-max-markets must be >= 1", file=sys.stderr)
        return 2
    if args.continuous and args.market_title:
        print("Error: --market-title is not supported with --continuous", file=sys.stderr)
        return 2
    if args.telegram_batch_size < 1:
        print("Error: --telegram-batch-size must be >= 1", file=sys.stderr)
        return 2

    telegram_bot_token = str(args.telegram_bot_token or os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    telegram_chat_id = str(args.telegram_chat_id or os.getenv("TELEGRAM_CHAT_ID") or "").strip()
    telegram_enabled = bool(telegram_bot_token and telegram_chat_id)
    if bool(telegram_bot_token) != bool(telegram_chat_id):
        print(
            (
                "Error: Telegram requires both token and chat id. "
                "Provide --telegram-bot-token + --telegram-chat-id "
                "or set TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID."
            ),
            file=sys.stderr,
        )
        return 2

    analysis_enabled = not args.no_analysis
    scenario_min_bets: List[float] = []
    scenario_max_bets: List[float] = []
    scenario_max_prices: List[float] = []
    scenario_auto_min_bets = False
    scenario_auto_max_bets = False
    scenario_auto_max_prices = False
    scenario_flags_provided = (
        cli_flag_was_provided("--scenario-min-bets")
        or cli_flag_was_provided("--scenario-max-bets")
        or cli_flag_was_provided("--scenario-max-prices")
    )

    if analysis_enabled and sys.stdin.isatty() and not scenario_flags_provided and not args.continuous:
        (
            analysis_enabled,
            scenario_min_bets,
            scenario_max_bets,
            scenario_max_prices,
            scenario_auto_min_bets,
            scenario_auto_max_bets,
            scenario_auto_max_prices,
        ) = (
            prompt_analysis_setup_interactively(
                default_min_bets=DEFAULT_SCENARIO_MIN_BETS,
                default_max_bets=DEFAULT_SCENARIO_MAX_BETS,
                default_max_prices_input=DEFAULT_SCENARIO_MAX_PRICES_INPUT,
            )
        )

    if analysis_enabled and not (
        scenario_min_bets
        or scenario_max_bets
        or scenario_max_prices
        or scenario_auto_min_bets
        or scenario_auto_max_bets
        or scenario_auto_max_prices
    ):
        try:
            scenario_min_bets, scenario_auto_min_bets = parse_min_bets_input(
                args.scenario_min_bets
            )
            scenario_max_bets, scenario_auto_max_bets = parse_max_bets_input(
                args.scenario_max_bets
            )
            scenario_max_prices, scenario_auto_max_prices = parse_max_prices_input(
                args.scenario_max_prices
            )
        except ValueError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 2

    types = None
    if args.types:
        types = [part.strip().upper() for part in args.types.split(",") if part.strip()]

    if args.continuous:
        if types is not None:
            print(
                "Warning: --types is ignored in --continuous mode (uses TRADE,REDEEM).",
                file=sys.stderr,
            )
        if args.side is not None:
            print(
                "Warning: --side is ignored in --continuous mode (captures BUY, SELL, REDEEM).",
                file=sys.stderr,
            )
        if args.telegram_send_existing and not telegram_enabled:
            print(
                "Warning: --telegram-send-existing is ignored (Telegram is not configured).",
                file=sys.stderr,
            )
    else:
        if telegram_enabled:
            print(
                "Warning: Telegram settings are used only in --continuous mode and will be ignored.",
                file=sys.stderr,
            )
        if args.telegram_send_existing:
            print(
                "Warning: --telegram-send-existing applies only in --continuous mode and will be ignored.",
                file=sys.stderr,
            )

    try:
        wallet_input = prompt_for_wallet(args.wallet)
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2

    session = session_with_headers()
    selected_market_titles: Optional[List[str]] = [args.market_title] if args.market_title else None
    output_path = args.output
    all_rows: List[Dict[str, Any]] = []
    analysis_result: Optional[Dict[str, Any]] = None

    try:
        wallet = resolve_wallet(session, wallet_input, args.timeout)
        print(f"Resolved wallet: {wallet}", file=sys.stderr)

        if args.continuous:
            return run_continuous_collection(
                session=session,
                wallet=wallet,
                output_base_path=args.output,
                timeout=args.timeout,
                page_limit=args.limit,
                poll_seconds=args.poll_seconds,
                finalize_grace_seconds=args.finalize_grace_seconds,
                discovery_pages=args.continuous_discovery_pages,
                max_markets=args.continuous_max_markets,
                state_path=args.continuous_state_file,
                analysis_enabled=analysis_enabled,
                scenario_min_bets=scenario_min_bets,
                scenario_max_bets=scenario_max_bets,
                scenario_max_prices=scenario_max_prices,
                scenario_auto_min_bets=scenario_auto_min_bets,
                scenario_auto_max_bets=scenario_auto_max_bets,
                scenario_auto_max_prices=scenario_auto_max_prices,
                start_ts=start_ts,
                end_ts=end_ts,
                max_pages=args.max_pages,
                telegram_bot_token=telegram_bot_token if telegram_enabled else None,
                telegram_chat_id=telegram_chat_id if telegram_enabled else None,
                telegram_batch_size=args.telegram_batch_size,
                telegram_send_existing=args.telegram_send_existing,
            )

        if not selected_market_titles and not args.no_interactive and sys.stdin.isatty():
            preview_rows = fetch_activity(
                session=session,
                wallet=wallet,
                page_limit=args.limit,
                timeout=args.timeout,
                max_pages=1,
                types=types,
                side=args.side,
                start_ts=start_ts,
                end_ts=end_ts,
                sort_by=args.sort_by,
                sort_direction=args.sort_direction,
            )
            selected_market_titles = choose_market_interactively(
                preview_rows,
                limit=args.recent_markets,
            )
            if selected_market_titles:
                if len(selected_market_titles) == 1:
                    print(f"Selected market: {selected_market_titles[0]}", file=sys.stderr)
                else:
                    print(
                        f"Selected {len(selected_market_titles)} markets: "
                        + "; ".join(selected_market_titles),
                        file=sys.stderr,
                    )
            else:
                print("Selected market: all markets", file=sys.stderr)

        output_path = output_path_with_market_label(args.output, selected_market_titles)
        if output_path != args.output:
            print(f"Output filename includes market label: {output_path}", file=sys.stderr)

        rows = fetch_activity(
            session=session,
            wallet=wallet,
            page_limit=args.limit,
            timeout=args.timeout,
            max_pages=args.max_pages,
            types=types,
            side=args.side,
            start_ts=start_ts,
            end_ts=end_ts,
            sort_by=args.sort_by,
            sort_direction=args.sort_direction,
        )
        all_rows = rows
        pre_filter_count = len(all_rows)
        rows = filter_rows_by_market_titles(all_rows, selected_market_titles, args.market_match)
        if selected_market_titles:
            market_scope = (
                "1 market" if len(selected_market_titles) == 1 else f"{len(selected_market_titles)} markets"
            )
            print(
                (
                    f"Applied market title filter ({args.market_match}, {market_scope}): "
                    f"{len(rows)} of {pre_filter_count} rows matched"
                ),
                file=sys.stderr,
            )
        count = write_csv(rows, output_path)
        if analysis_enabled:
            analysis_result = generate_analysis_files(
                rows=rows,
                wallet=wallet,
                selected_market_titles=selected_market_titles,
                output_csv_path=output_path,
                scenario_min_bets=scenario_min_bets,
                scenario_max_bets=scenario_max_bets,
                scenario_max_prices=scenario_max_prices,
                scenario_auto_min_bets=scenario_auto_min_bets,
                scenario_auto_max_bets=scenario_auto_max_bets,
                scenario_auto_max_prices=scenario_auto_max_prices,
            )
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "unknown"
        print(f"HTTP error ({status}): {exc}", file=sys.stderr)
        return 1
    except requests.RequestException as exc:
        print(f"Network error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    if selected_market_titles and count == 0:
        # Show likely titles from the unfiltered data to help the user pick the exact value.
        suggestions = summarize_market_titles(all_rows)
        if suggestions:
            print("No rows matched --market-title. Example titles from the wallet:", file=sys.stderr)
            for title in suggestions:
                print(f"  - {title}", file=sys.stderr)

    print(f"Wrote {count} rows to {output_path}")
    if analysis_result is not None:
        print(f"Wrote analysis report to {analysis_result['analysis_path']}")
        print(f"Wrote scenario table to {analysis_result['scenarios_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
