import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def load_env_file(path: str) -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            # Always let project-local .env values win over inherited shell env.
            if key:
                os.environ[key] = value


@dataclass
class RuntimeConfig:
    api_key: str
    api_secret: str
    trading_base_url: str
    data_base_url: str


def build_runtime_config() -> RuntimeConfig:
    load_env_file(".env")
    api_key = os.getenv("ALPACA_API_KEY", "").strip()
    api_secret = os.getenv("ALPACA_API_SECRET", "").strip()
    trading_base_url = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets").strip().rstrip("/")
    data_base_url = os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").strip().rstrip("/")
    if not api_key or not api_secret:
        raise ValueError("ALPACA_API_KEY and ALPACA_API_SECRET are required.")
    return RuntimeConfig(
        api_key=api_key,
        api_secret=api_secret,
        trading_base_url=trading_base_url,
        data_base_url=data_base_url,
    )


def load_json(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def ensure_risk_policy(path: str = "config/risk_policy.json") -> dict[str, Any]:
    policy_path = Path(path)
    if policy_path.exists():
        return load_json(str(policy_path))
    return load_json("config/risk_policy.example.json")
