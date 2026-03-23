import json
import os
import sys
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def load_env_file(path: str) -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key:
                os.environ[key] = value


def main() -> int:
    load_env_file(".env")

    api_key = os.getenv("ALPACA_API_KEY", "").strip()
    api_secret = os.getenv("ALPACA_API_SECRET", "").strip()
    base_url = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets").strip().rstrip("/")

    if not api_key or not api_secret:
        print("Missing ALPACA_API_KEY or ALPACA_API_SECRET.")
        print("Set them in environment variables or a local .env file.")
        return 1

    # Accept ALPACA_BASE_URL with or without a trailing /v2.
    if base_url.endswith("/v2"):
        url = f"{base_url}/account"
    else:
        url = f"{base_url}/v2/account"
    headers = {
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": api_secret,
        "Accept": "application/json",
    }
    req = Request(url=url, headers=headers, method="GET")

    try:
        with urlopen(req, timeout=20) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            print("Alpaca connection successful.")
            print(f"Account ID: {payload.get('id', 'unknown')}")
            print(f"Status: {payload.get('status', 'unknown')}")
            print(f"Currency: {payload.get('currency', 'unknown')}")
            print(f"Buying Power: {payload.get('buying_power', 'unknown')}")
            return 0
    except HTTPError as err:
        body = err.read().decode("utf-8", errors="replace")
        print(f"HTTP error: {err.code} {err.reason}")
        if body:
            print(body)
        return 2
    except URLError as err:
        print(f"Network error: {err.reason}")
        return 3
    except Exception as err:  # pragma: no cover
        print(f"Unexpected error: {err}")
        return 4


if __name__ == "__main__":
    sys.exit(main())
