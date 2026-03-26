from __future__ import annotations

import base64
import hashlib
import hmac
import os
import time
import urllib.parse
from typing import Any

import requests

from .config import load_env_file

# Kraken pair format: "XXBTZUSD" for BTC/USD, but the REST API also accepts
# "XBTUSD" or the websocket-style "XBT/USD".  We normalise our internal
# "BTC/USD" style to whatever Kraken needs at each endpoint.

_ALPACA_TO_KRAKEN: dict[str, str] = {
    "BTC/USD": "XXBTZUSD",
    "ETH/USD": "XETHZUSD",
    "SOL/USD": "SOLUSD",
    "AVAX/USD": "AVAXUSD",
    "LTC/USD": "XLTCZUSD",
    "LINK/USD": "LINKUSD",
    "BCH/USD": "BCHUSD",
    "UNI/USD": "UNIUSD",
    "AAVE/USD": "AAVEUSD",
    "DOGE/USD": "XDGUSD",
    "DOT/USD": "DOTUSD",
    "SHIB/USD": "SHIBUSD",
    "XTZ/USD": "XTZUSD",
    "MKR/USD": "MKRUSD",
    "GRT/USD": "GRTUSD",
    "BAT/USD": "BATUSD",
    "CRV/USD": "CRVUSD",
    "SUSHI/USD": "SUSHIUSD",
    "ALGO/USD": "ALGOUSD",
    "BABY/USD": "BABYUSD",
    "XRP/USD": "XXRPZUSD",
}

_KRAKEN_TO_ALPACA: dict[str, str] = {v: k for k, v in _ALPACA_TO_KRAKEN.items()}


def _kraken_pair(alpaca_symbol: str, dynamic_map: dict[str, str] | None = None) -> str:
    if dynamic_map and alpaca_symbol in dynamic_map:
        return dynamic_map[alpaca_symbol]
    return _ALPACA_TO_KRAKEN.get(alpaca_symbol, alpaca_symbol.replace("/", ""))


def _alpaca_pair(kraken_symbol: str, dynamic_map: dict[str, str] | None = None) -> str:
    if dynamic_map and kraken_symbol in dynamic_map:
        return dynamic_map[kraken_symbol]
    if kraken_symbol in _KRAKEN_TO_ALPACA:
        return _KRAKEN_TO_ALPACA[kraken_symbol]
    return kraken_symbol


class KrakenClient:
    BASE = "https://api.kraken.com"

    def __init__(self, timeout: int = 20) -> None:
        load_env_file(".env")
        self.api_key = os.getenv("KRAKEN_API_KEY", "").strip()
        self.api_secret = os.getenv("KRAKEN_API_SECRET", "").strip()
        self.timeout = timeout
        if not self.api_key or not self.api_secret:
            raise ValueError("KRAKEN_API_KEY and KRAKEN_API_SECRET are required.")
        self._pair_decimals: dict[str, int] = {}
        self._lot_decimals: dict[str, int] = {}
        self._usd_pairs: dict[str, dict[str, Any]] = {}
        self._kraken_to_friendly: dict[str, str] = {}
        self._friendly_to_kraken: dict[str, str] = {}
        self._asset_to_symbol: dict[str, str] = {}
        self._load_pair_metadata()

    _EXCLUDED_BASES = {
        "USDT", "USDC", "DAI", "USDG", "PYUSD", "TUSD", "BUSD", "GUSD",
        "FRAX", "LUSD", "SUSD", "ZUSD", "USD", "USDD", "USDP", "USDE",
        "USDS", "USD1", "AUSD", "EURC", "EURR", "EURQ",
        "TGBP", "ZGBP", "BRL1", "MXNB", "RLUSD", "USUAL",
        "PAXG", "XAUT", "TBTC", "WBTC", "WBT", "LSETH", "CMETH",
        "MSOL", "LSSOL", "JITOSOL", "STETH",
        "EUR", "GBP", "AUD", "CAD", "CHF", "JPY",
        "ZEUR", "ZGBP", "ZAUD", "ZCAD", "ZJPY",
    }

    def _asset_to_alpaca_symbol(self, asset: str) -> str | None:
        return self._asset_to_symbol.get(asset)

    def _load_pair_metadata(self) -> None:
        try:
            result = self._public("AssetPairs")
        except Exception:
            return
        for pair_name, info in result.items():
            self._pair_decimals[pair_name] = int(info.get("pair_decimals", 6))
            self._lot_decimals[pair_name] = int(info.get("lot_decimals", 8))
            quote = info.get("quote", "")
            if quote not in ("ZUSD", "USD") or info.get("status") != "online":
                continue
            wsname = info.get("wsname", "")
            base = info.get("base", "")
            if not wsname or "/" not in wsname:
                continue
            friendly = wsname.replace("XBT", "BTC")
            if not friendly.endswith("/USD"):
                continue
            base_ticker = friendly.split("/")[0]
            if base_ticker in self._EXCLUDED_BASES:
                continue
            self._usd_pairs[pair_name] = info
            self._kraken_to_friendly[pair_name] = friendly
            self._friendly_to_kraken[friendly] = pair_name
            self._asset_to_symbol[base] = friendly

    def discover_tradeable_pairs(
        self,
        min_24h_volume_usd: float = 50_000.0,
        top_n: int = 80,
    ) -> list[dict[str, Any]]:
        """Scan all USD pairs via Ticker, rank by 24h volume, return top candidates."""
        all_kraken_names = list(self._usd_pairs.keys())
        if not all_kraken_names:
            return []
        batch_size = 40
        scored: list[dict[str, Any]] = []
        for i in range(0, len(all_kraken_names), batch_size):
            batch = all_kraken_names[i : i + batch_size]
            try:
                tickers = self._public("Ticker", {"pair": ",".join(batch)})
            except Exception:
                continue
            for kraken_name, data in tickers.items():
                friendly = self._kraken_to_friendly.get(kraken_name)
                if not friendly:
                    continue
                try:
                    last_price = float(data["c"][0])
                    vol_24h = float(data["v"][1])
                    vwap_24h = float(data["p"][1])
                    open_24h = float(data["o"])
                except (KeyError, IndexError, ValueError):
                    continue
                vol_usd = vol_24h * vwap_24h if vwap_24h > 0 else vol_24h * last_price
                if vol_usd < min_24h_volume_usd or last_price <= 0:
                    continue
                change_pct = ((last_price - open_24h) / open_24h * 100) if open_24h > 0 else 0
                high_24h = float(data.get("h", [0, 0])[1])
                low_24h = float(data.get("l", [0, 0])[1])
                range_pct = ((high_24h - low_24h) / low_24h * 100) if low_24h > 0 else 0
                scored.append({
                    "symbol": friendly,
                    "kraken_pair": kraken_name,
                    "price": last_price,
                    "volume_usd_24h": vol_usd,
                    "change_pct_24h": change_pct,
                    "range_pct_24h": range_pct,
                    "high_24h": high_24h,
                    "low_24h": low_24h,
                })
        scored.sort(key=lambda x: x["volume_usd_24h"], reverse=True)
        return scored[:top_n]

    def _format_price(self, kraken_pair: str, price: float) -> str:
        decimals = self._pair_decimals.get(kraken_pair, 2)
        return f"{price:.{decimals}f}"

    def _format_volume(self, kraken_pair: str, volume: float) -> str:
        decimals = self._lot_decimals.get(kraken_pair, 8)
        return f"{volume:.{decimals}f}".rstrip("0").rstrip(".")

    def _sign(self, url_path: str, data: dict[str, Any]) -> str:
        encoded = (str(data["nonce"]) + urllib.parse.urlencode(data)).encode()
        message = url_path.encode() + hashlib.sha256(encoded).digest()
        mac = hmac.new(base64.b64decode(self.api_secret), message, hashlib.sha512)
        return base64.b64encode(mac.digest()).decode()

    def _public(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{self.BASE}/0/public/{endpoint}"
        resp = requests.get(url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        body = resp.json()
        if body.get("error"):
            raise RuntimeError(f"Kraken API error: {body['error']}")
        return body.get("result", {})

    def _private(self, endpoint: str, data: dict[str, Any] | None = None) -> Any:
        url_path = f"/0/private/{endpoint}"
        url = f"{self.BASE}{url_path}"
        payload = dict(data or {})
        payload["nonce"] = str(time.time_ns())
        headers = {
            "API-Key": self.api_key,
            "API-Sign": self._sign(url_path, payload),
            "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
        }
        resp = requests.post(url, headers=headers, data=urllib.parse.urlencode(payload), timeout=self.timeout)
        resp.raise_for_status()
        body = resp.json()
        if body.get("error"):
            raise RuntimeError(f"Kraken API error: {body['error']}")
        return body.get("result", {})

    # ── Account ────────────────────────────────────────────────────────

    def get_account(self) -> dict[str, Any]:
        """Return account-like dict compatible with Alpaca shape."""
        balance = self._private("Balance")
        usd = float(balance.get("ZUSD", balance.get("USD", 0)))
        total = sum(float(v) for v in balance.values())
        return {
            "status": "ACTIVE",
            "equity": str(total),
            "last_equity": str(total),
            "cash": str(usd),
            "buying_power": str(usd),
            "currency": "USD",
            "pattern_day_trader": False,
            "daytrade_count": 0,
        }

    def get_open_positions(self) -> list[dict[str, Any]]:
        """Return Alpaca-shaped position list from Kraken balances."""
        balance = self._private("Balance")
        avg_entry_map = self.get_avg_entry_prices()
        positions: list[dict[str, Any]] = []
        for asset, qty_str in balance.items():
            qty = float(qty_str)
            if qty <= 0 or asset in ("ZUSD", "USD"):
                continue
            alpaca_sym = self._asset_to_alpaca_symbol(asset)
            if not alpaca_sym:
                continue
            price = self.get_latest_price(alpaca_sym)
            avg_entry = float(avg_entry_map.get(alpaca_sym, 0.0))
            positions.append({
                "symbol": alpaca_sym,
                "qty": str(qty),
                "market_value": str(qty * (price or 0)),
                "avg_entry_price": str(avg_entry),
                "current_price": str(price or 0),
                "side": "long",
            })
        return positions

    def get_avg_entry_prices(self, symbols: list[str] | None = None) -> dict[str, float]:
        """Estimate average entry prices for current spot holdings via FIFO trades."""
        symbol_filter = set(symbols or [])
        avg_prices: dict[str, float] = {}
        try:
            trades_raw = self._private("TradesHistory").get("trades", {})
        except Exception:
            return avg_prices

        # FIFO lots by symbol -> list[(qty, price)]
        lots_by_symbol: dict[str, list[tuple[float, float]]] = {}
        rows = sorted(
            trades_raw.values(),
            key=lambda x: float(x.get("time", 0.0) or 0.0),
        )
        for info in rows:
            pair = _alpaca_pair(str(info.get("pair", "")), self._kraken_to_friendly)
            if not pair or (symbol_filter and pair not in symbol_filter):
                continue
            try:
                qty = float(info.get("vol", 0.0) or 0.0)
                price = float(info.get("price", 0.0) or 0.0)
            except (TypeError, ValueError):
                continue
            if qty <= 0 or price <= 0:
                continue
            side = str(info.get("type", "")).lower()
            lots = lots_by_symbol.setdefault(pair, [])
            if side == "buy":
                lots.append((qty, price))
                continue
            if side != "sell":
                continue
            remaining = qty
            while remaining > 0 and lots:
                lot_qty, lot_price = lots[0]
                take = min(remaining, lot_qty)
                lot_qty -= take
                remaining -= take
                if lot_qty <= 1e-9:
                    lots.pop(0)
                else:
                    lots[0] = (lot_qty, lot_price)

        for pair, lots in lots_by_symbol.items():
            total_qty = sum(q for q, _ in lots)
            if total_qty <= 1e-9:
                continue
            total_cost = sum(q * p for q, p in lots)
            avg_prices[pair] = total_cost / total_qty
        return avg_prices

    @staticmethod
    def _asset_to_alpaca_symbol(kraken_asset: str) -> str | None:
        asset_map = {
            "XXBT": "BTC/USD", "XBT": "BTC/USD",
            "XETH": "ETH/USD", "ETH": "ETH/USD",
            "SOL": "SOL/USD",
            "AVAX": "AVAX/USD",
            "XLTC": "LTC/USD", "LTC": "LTC/USD",
            "LINK": "LINK/USD",
            "BCH": "BCH/USD",
            "UNI": "UNI/USD",
            "AAVE": "AAVE/USD",
            "XXDG": "DOGE/USD", "XDG": "DOGE/USD",
            "DOT": "DOT/USD",
            "SHIB": "SHIB/USD",
            "XTZ": "XTZ/USD",
            "MKR": "MKR/USD",
            "GRT": "GRT/USD",
            "BAT": "BAT/USD",
            "CRV": "CRV/USD",
            "SUSHI": "SUSHI/USD",
            "ALGO": "ALGO/USD",
        }
        return asset_map.get(kraken_asset)

    # ── Orders ─────────────────────────────────────────────────────────

    def get_orders(
        self,
        status: str = "all",
        limit: int = 100,
        direction: str = "desc",
        **_kwargs: Any,
    ) -> list[dict[str, Any]]:
        if status == "open":
            result = self._private("OpenOrders")
            raw = result.get("open", {})
        else:
            result = self._private("ClosedOrders")
            raw = result.get("closed", {})
        orders: list[dict[str, Any]] = []
        for txid, info in raw.items():
            descr = info.get("descr", {})
            orders.append({
                "id": txid,
                "symbol": _alpaca_pair(descr.get("pair", ""), self._kraken_to_friendly),
                "side": descr.get("type", "buy"),
                "qty": str(info.get("vol", "0")),
                "filled_qty": str(info.get("vol_exec", "0")),
                "status": info.get("status", "open"),
                "created_at": info.get("opentm"),
                "submitted_at": info.get("opentm"),
                "order_type": descr.get("ordertype", "limit"),
                "limit_price": descr.get("price", "0"),
            })
        orders.sort(key=lambda o: float(o.get("created_at") or 0), reverse=(direction == "desc"))
        return orders[:limit]

    def place_order(
        self,
        symbol: str,
        qty: int | float | str,
        side: str = "buy",
        order_type: str = "limit",
        tif: str = "gtc",
        limit_price: float | None = None,
        take_profit_price: float | None = None,
        stop_loss_price: float | None = None,
    ) -> dict[str, Any]:
        pair = _kraken_pair(symbol, self._friendly_to_kraken)
        if isinstance(qty, str):
            volume = qty
        elif isinstance(qty, float):
            volume = self._format_volume(pair, qty)
        else:
            volume = str(qty)

        payload: dict[str, Any] = {
            "pair": pair,
            "type": side.lower(),
            "ordertype": "limit" if order_type == "limit" else "market",
            "volume": volume,
        }
        if order_type == "limit" and limit_price is not None:
            payload["price"] = self._format_price(pair, limit_price)
        result = self._private("AddOrder", payload)
        txids = result.get("txid", [])
        return {
            "id": txids[0] if txids else "",
            "symbol": symbol,
            "side": side,
            "qty": volume,
            "order_type": order_type,
            "limit_price": limit_price,
            "status": "new",
            "broker": "kraken",
        }

    def cancel_order(self, order_id: str) -> dict[str, Any]:
        result = self._private("CancelOrder", {"txid": order_id})
        return {"id": order_id, "cancelled": True, "result": result}

    # ── Market Data ────────────────────────────────────────────────────

    def get_crypto_bars(
        self,
        symbols: list[str],
        timeframe: str = "1Hour",
        limit: int = 80,
    ) -> dict[str, list[dict[str, Any]]]:
        interval_map = {
            "1Min": 1, "5Min": 5, "15Min": 15, "30Min": 30,
            "1Hour": 60, "4Hour": 240, "1Day": 1440,
        }
        interval = interval_map.get(timeframe, 60)
        bars_out: dict[str, list[dict[str, Any]]] = {}
        for sym in symbols:
            pair = _kraken_pair(sym, self._friendly_to_kraken)
            try:
                result = self._public("OHLC", {"pair": pair, "interval": interval})
                pair_data = None
                for key, val in result.items():
                    if key != "last" and isinstance(val, list):
                        pair_data = val
                        break
                if not pair_data:
                    bars_out[sym] = []
                    continue
                entries = pair_data[-limit:] if len(pair_data) > limit else pair_data
                bars: list[dict[str, Any]] = []
                for entry in entries:
                    # Kraken OHLC: [time, open, high, low, close, vwap, volume, count]
                    if len(entry) < 7:
                        continue
                    bars.append({
                        "t": entry[0],
                        "o": float(entry[1]),
                        "h": float(entry[2]),
                        "l": float(entry[3]),
                        "c": float(entry[4]),
                        "v": float(entry[6]),
                    })
                bars_out[sym] = bars
            except Exception:
                bars_out[sym] = []
        return bars_out

    def get_crypto_latest_prices(self, symbols: list[str]) -> dict[str, float]:
        prices: dict[str, float] = {}
        pairs = [_kraken_pair(s, self._friendly_to_kraken) for s in symbols]
        try:
            result = self._public("Ticker", {"pair": ",".join(pairs)})
            for kraken_pair, data in result.items():
                alpaca_sym = _alpaca_pair(kraken_pair, self._kraken_to_friendly)
                if not alpaca_sym:
                    for s, kp in zip(symbols, pairs):
                        if kp == kraken_pair or kraken_pair.startswith(kp):
                            alpaca_sym = s
                            break
                if alpaca_sym:
                    last_trade = data.get("c", [])
                    if last_trade:
                        prices[alpaca_sym] = float(last_trade[0])
        except Exception:
            pass
        return prices

    def get_latest_price(self, symbol: str) -> float | None:
        return self.get_crypto_latest_prices([symbol]).get(symbol)

    # ── Stock stubs (not used for crypto-only routing) ─────────────────

    def get_stock_bars(self, symbols: list[str], timeframe: str = "1Day", limit: int = 40) -> dict[str, list[dict[str, Any]]]:
        return {}

    def get_stock_snapshots(self, symbols: list[str]) -> dict[str, Any]:
        return {}

    def get_stock_latest_prices(self, symbols: list[str]) -> dict[str, float]:
        return {}

    def get_portfolio_history(self, **_kwargs: Any) -> dict[str, Any]:
        return {}
