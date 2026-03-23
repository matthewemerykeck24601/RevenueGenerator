from __future__ import annotations

import json
from typing import Any

import requests

from .config import RuntimeConfig


class AlpacaClient:
    def __init__(self, cfg: RuntimeConfig, timeout: int = 20) -> None:
        self.cfg = cfg
        self.timeout = timeout
        self.headers = {
            "APCA-API-KEY-ID": cfg.api_key,
            "APCA-API-SECRET-KEY": cfg.api_secret,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _trading_url(self, path: str) -> str:
        base = self.cfg.trading_base_url
        if base.endswith("/v2"):
            return f"{base}{path}"
        return f"{base}/v2{path}"

    def _get(self, url: str, params: dict[str, Any] | None = None) -> Any:
        resp = requests.get(url, headers=self.headers, params=params, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def _post(self, url: str, payload: dict[str, Any]) -> Any:
        resp = requests.post(url, headers=self.headers, data=json.dumps(payload), timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def _delete(self, url: str) -> Any:
        resp = requests.delete(url, headers=self.headers, timeout=self.timeout)
        if resp.status_code == 204 or not resp.text.strip():
            return {}
        resp.raise_for_status()
        return resp.json()

    def get_account(self) -> dict[str, Any]:
        return self._get(self._trading_url("/account"))

    def get_open_positions(self) -> list[dict[str, Any]]:
        return self._get(self._trading_url("/positions"))

    def get_orders(self, status: str = "all", limit: int = 100, direction: str = "desc") -> list[dict[str, Any]]:
        return self._get(
            self._trading_url("/orders"),
            params={
                "status": status,
                "limit": limit,
                "direction": direction,
                "nested": "false",
            },
        )

    def get_portfolio_history(
        self,
        *,
        period: str = "1D",
        timeframe: str = "5Min",
        extended_hours: bool = True,
    ) -> dict[str, Any]:
        return self._get(
            self._trading_url("/account/portfolio/history"),
            params={
                "period": period,
                "timeframe": timeframe,
                "extended_hours": str(extended_hours).lower(),
                "pnl_reset": "no_reset",
                "intraday_reporting": "continuous",
            },
        )

    def place_order(
        self,
        symbol: str,
        qty: int | float | str,
        side: str = "buy",
        order_type: str = "limit",
        tif: str = "day",
        limit_price: float | None = None,
        take_profit_price: float | None = None,
        stop_loss_price: float | None = None,
    ) -> dict[str, Any]:
        if isinstance(qty, str):
            qty_value = qty
        elif isinstance(qty, float):
            qty_value = f"{qty:.6f}".rstrip("0").rstrip(".")
        else:
            qty_value = str(qty)
        payload: dict[str, Any] = {
            "symbol": symbol,
            "qty": qty_value,
            "side": side,
            "type": order_type,
            "time_in_force": tif,
        }
        if order_type == "limit" and limit_price is not None:
            payload["limit_price"] = f"{limit_price:.2f}"
        if take_profit_price is not None and stop_loss_price is not None and "/" not in symbol:
            payload["order_class"] = "bracket"
            payload["take_profit"] = {"limit_price": f"{take_profit_price:.2f}"}
            payload["stop_loss"] = {"stop_price": f"{stop_loss_price:.2f}"}
        return self._post(self._trading_url("/orders"), payload=payload)

    def cancel_order(self, order_id: str) -> dict[str, Any]:
        return self._delete(self._trading_url(f"/orders/{order_id}"))

    def get_stock_bars(self, symbols: list[str], timeframe: str = "1Day", limit: int = 40) -> dict[str, list[dict[str, Any]]]:
        url = f"{self.cfg.data_base_url}/v2/stocks/bars"
        data = self._get(
            url,
            params={
                "symbols": ",".join(symbols),
                "timeframe": timeframe,
                "limit": limit,
                "adjustment": "raw",
                "feed": "iex",
            },
        )
        return data.get("bars", {})

    def get_stock_snapshots(self, symbols: list[str]) -> dict[str, Any]:
        url = f"{self.cfg.data_base_url}/v2/stocks/snapshots"
        return self._get(url, params={"symbols": ",".join(symbols)})

    def get_stock_latest_prices(self, symbols: list[str]) -> dict[str, float]:
        snapshots = self.get_stock_snapshots(symbols)
        prices: dict[str, float] = {}
        for symbol, snap in snapshots.items():
            latest_trade = snap.get("latestTrade", {})
            if latest_trade.get("p") is not None:
                prices[symbol] = float(latest_trade["p"])
                continue
            daily_bar = snap.get("dailyBar", {})
            if daily_bar.get("c") is not None:
                prices[symbol] = float(daily_bar["c"])
        return prices

    def get_crypto_bars(
        self,
        symbols: list[str],
        timeframe: str = "1Hour",
        limit: int = 80,
    ) -> dict[str, list[dict[str, Any]]]:
        url = f"{self.cfg.data_base_url}/v1beta3/crypto/us/bars"
        data = self._get(
            url,
            params={
                "symbols": ",".join(symbols),
                "timeframe": timeframe,
                "limit": limit,
            },
        )
        return data.get("bars", {})

    def get_crypto_latest_prices(self, symbols: list[str]) -> dict[str, float]:
        bars = self.get_crypto_bars(symbols=symbols, timeframe="1Min", limit=1)
        prices: dict[str, float] = {}
        for symbol, items in bars.items():
            if not items:
                continue
            prices[symbol] = float(items[-1]["c"])
        return prices

    def get_latest_price(self, symbol: str) -> float | None:
        if "/" in symbol:
            return self.get_crypto_latest_prices([symbol]).get(symbol)
        return self.get_stock_latest_prices([symbol]).get(symbol)
