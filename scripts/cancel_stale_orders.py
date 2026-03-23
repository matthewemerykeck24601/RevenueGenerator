import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from revenue_generator.alpaca_client import AlpacaClient
from revenue_generator.config import build_runtime_config


def _parse_iso_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Cancel stale open buy orders.")
    p.add_argument("--minutes", type=int, default=20, help="Cancel open buy orders older than this many minutes.")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=max(args.minutes, 1))
    client = AlpacaClient(cfg=build_runtime_config())

    cancelled = 0
    open_orders = client.get_orders(status="open", limit=500, direction="desc")
    for order in open_orders:
        if str(order.get("side", "")).lower() != "buy":
            continue
        order_id = order.get("id")
        if not order_id:
            continue
        created_at = _parse_iso_utc(order.get("created_at") or order.get("submitted_at"))
        if not created_at or created_at > cutoff:
            continue
        try:
            client.cancel_order(str(order_id))
            cancelled += 1
        except Exception:
            continue

    print(f"Cancelled {cancelled} stale open buy orders older than {args.minutes} minutes.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
