from __future__ import annotations

import csv
from pathlib import Path


def read_rows(path: Path) -> list[dict[str, float | str]]:
    out: list[dict[str, float | str]] = []
    with path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            out.append(
                {
                    "min_bet_usdc": float(row["min_bet_usdc"]),
                    "max_bet_usdc": float(row["max_bet_usdc"]),
                    "max_price": float(row["max_price"]),
                    "net_pnl": float(row["net_pnl"]),
                    "roi_pct": float(row["roi_pct"]),
                    "spend_usdc": float(row["spend_usdc"]),
                }
            )
    return out


def find_nearest(
    rows: list[dict[str, float | str]],
    min_bet: float,
    max_bet: float,
    max_price: float,
) -> dict[str, float | str]:
    best = None
    best_dist = None
    for row in rows:
        dist = (
            (float(row["min_bet_usdc"]) - min_bet) ** 2
            + ((float(row["max_bet_usdc"]) - max_bet) / 20.0) ** 2
            + ((float(row["max_price"]) - max_price) / 0.1) ** 2
        ) ** 0.5
        if best is None or dist < best_dist:
            best = row
            best_dist = dist
    assert best is not None
    best["dist"] = best_dist
    return best


def main() -> None:
    base = Path(r"C:\Users\fomin\OneDrive\Documents\polymarket\ActivityScraper")
    files = {
        "yo-4": base / "yo" / "polymarket_activity - 4 markets_scenarios.csv",
        "yo-3": base / "yo" / "polymarket_activity - 3 markets_scenarios.csv",
        "yo-ye-3": base / "yo" / "ye" / "polymarket_activity - 3 markets_scenarios.csv",
    }
    data = {name: read_rows(path) for name, path in files.items()}

    candidates = [
        (0.0, 20.0, 0.60),
        (0.0, 40.0, 0.60),
        (5.0, 20.0, 0.60),
        (10.0, 20.0, 0.60),
        (1.0, 40.0, 0.60),
        (0.0, 20.0, 0.65),
    ]

    for min_bet, max_bet, max_price in candidates:
        print(f"\nCandidate ({min_bet}, {max_bet}, {max_price})")
        total_net = 0.0
        for name, rows in data.items():
            row = find_nearest(rows, min_bet=min_bet, max_bet=max_bet, max_price=max_price)
            total_net += float(row["net_pnl"])
            print(
                f"  {name}: nearest=({row['min_bet_usdc']:.2f},{row['max_bet_usdc']:.2f},{row['max_price']:.2f}), "
                f"net={row['net_pnl']:.2f}, roi={row['roi_pct']:.2f}%, spend={row['spend_usdc']:.2f}, dist={row['dist']:.3f}"
            )
        print(f"  aggregate net sum: {total_net:.2f}")


if __name__ == "__main__":
    main()
