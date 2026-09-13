"""Fetch current crypto market data from six public market-data APIs.

Every function has the same shape:

    fetch_<provider>(api_key, symbols=SYMBOLS) -> list[dict]

and returns one normalised record per coin, in the order of `symbols`, so the
results can be compared side by side (or handed straight to the Kafka producer):

    {
        "source":     provider name,
        "symbol":     asset symbol, e.g. "BTC",
        "currency":   quote currency, e.g. "USD",
        "price":      float, the current market price,
        "timestamp":  UTC ISO-8601 string reported by the API,
        "fetched_at": UTC ISO-8601 string, when we made the call,
        "extra":      dict of whatever else that particular API offers,
        "raw":        the provider's JSON for this coin, untouched,
    }
"""

# This file is called requests.py, which shadows the `requests` library whenever
# the script's own directory ends up first on sys.path (i.e. `python src/requests.py`).
# Dropping that directory before the import makes the real library win.
# The clean fix is to rename this file to something like http_requests.py.
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path[:] = [p for p in sys.path if os.path.abspath(p or ".") != _HERE]

from datetime import datetime, timezone

import requests

DEFAULT_TIMEOUT = 10

# Each provider names coins differently: tickers, slugs or UUIDs.
# Add a row here to track another coin across every provider.
COINS = {
    "BTC": {"coincap": "bitcoin", "coingecko": "bitcoin", "coinranking": "Qwsogvtv82FCd", "coinpaprika": "btc-bitcoin"},
    "ETH": {"coincap": "ethereum", "coingecko": "ethereum", "coinranking": "razxDUgYGNAdQ", "coinpaprika": "eth-ethereum"},
    "BNB": {"coincap": "binance-coin", "coingecko": "binancecoin", "coinranking": "WcwrkfNI4FUAe", "coinpaprika": "bnb-binance-coin"},
    "XRP": {"coincap": "xrp", "coingecko": "ripple", "coinranking": "-l8Mn2pVlRs-p", "coinpaprika": "xrp-xrp"},
    "SOL": {"coincap": "solana", "coingecko": "solana", "coinranking": "zNZHO_Sjf", "coinpaprika": "sol-solana"},
}
SYMBOLS = tuple(COINS)


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _now_iso():
    """Current UTC time as an ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _iso(epoch, unit="s"):
    """Convert an epoch timestamp (seconds or milliseconds) to UTC ISO-8601."""
    if epoch is None:
        return None
    epoch = float(epoch) / (1000 if unit == "ms" else 1)
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat(timespec="seconds")


def _get(url, params=None, headers=None, timeout=DEFAULT_TIMEOUT):
    """GET a JSON endpoint and raise on transport or HTTP errors."""
    response = requests.get(url, params=params, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.json()


def _ids(provider, symbols):
    """Map canonical symbols to one provider's ids, as {provider_id: symbol}."""
    unknown = [s for s in symbols if s not in COINS]
    if unknown:
        raise KeyError(f"no id mapping for {unknown}; add them to COINS")
    return {COINS[s][provider]: s for s in symbols}


def _record(source, symbol, currency, price, timestamp, extra, raw):
    return {
        "source": source,
        "symbol": symbol,
        "currency": currency,
        "price": float(price) if price is not None else None,
        "timestamp": timestamp,
        "fetched_at": _now_iso(),
        "extra": extra,
        "raw": raw,
    }


# --------------------------------------------------------------------------- #
# providers
# --------------------------------------------------------------------------- #
def fetch_coinlayer(api_key, symbols=SYMBOLS, target="USD", timeout=DEFAULT_TIMEOUT):
    """Coinlayer live rates, one request for all coins.

    Key goes in the query string as `access_key`. The free plan is HTTP-only
    and quotes against USD only. Coinlayer offers nothing beyond the rate.
    """
    data = _get(
        "http://api.coinlayer.com/live",
        params={"access_key": api_key, "symbols": ",".join(symbols), "target": target},
        timeout=timeout,
    )
    if not data.get("success", False):
        raise RuntimeError(f"coinlayer error: {data.get('error')}")

    rates = data["rates"]
    return [
        _record(
            source="coinlayer",
            symbol=symbol,
            currency=data.get("target", target),
            price=rates.get(symbol),
            timestamp=_iso(data.get("timestamp")),
            extra={"target": data.get("target")},
            raw={"timestamp": data.get("timestamp"), "target": data.get("target"), "rate": rates.get(symbol)},
        )
        for symbol in symbols
    ]


def fetch_coinmarketcap(api_key, symbols=SYMBOLS, convert="USD", timeout=DEFAULT_TIMEOUT):
    """CoinMarketCap latest quotes, one request for all coins.

    Key goes in the `X-CMC_PRO_API_KEY` header.
    """
    data = _get(
        "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest",
        params={"symbol": ",".join(symbols), "convert": convert},
        headers={"X-CMC_PRO_API_KEY": api_key, "Accept": "application/json"},
        timeout=timeout,
    )
    status = data.get("status", {})
    if status.get("error_code"):
        raise RuntimeError(f"coinmarketcap error: {status.get('error_message')}")

    records = []
    for symbol in symbols:
        coin = data["data"][symbol]
        quote = coin["quote"][convert]
        records.append(
            _record(
                source="coinmarketcap",
                symbol=symbol,
                currency=convert,
                price=quote.get("price"),
                timestamp=quote.get("last_updated"),
                extra={
                    "name": coin.get("name"),
                    "cmc_rank": coin.get("cmc_rank"),
                    "market_cap": quote.get("market_cap"),
                    "market_cap_dominance": quote.get("market_cap_dominance"),
                    "fully_diluted_market_cap": quote.get("fully_diluted_market_cap"),
                    "volume_24h": quote.get("volume_24h"),
                    "volume_change_24h": quote.get("volume_change_24h"),
                    "percent_change_1h": quote.get("percent_change_1h"),
                    "percent_change_24h": quote.get("percent_change_24h"),
                    "percent_change_7d": quote.get("percent_change_7d"),
                    "circulating_supply": coin.get("circulating_supply"),
                    "total_supply": coin.get("total_supply"),
                    "max_supply": coin.get("max_supply"),
                },
                raw=coin,
            )
        )
    return records


def fetch_coincap(api_key, symbols=SYMBOLS, timeout=DEFAULT_TIMEOUT):
    """CoinCap assets, one request for all coins.

    Key goes in the `Authorization: Bearer` header. CoinCap addresses assets
    by slug (`bitcoin`, `binance-coin`), mapped from the symbol via COINS.
    """
    ids = _ids("coincap", symbols)
    data = _get(
        "https://rest.coincap.io/v3/assets",
        params={"ids": ",".join(ids)},
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=timeout,
    )
    by_id = {coin["id"]: coin for coin in data["data"]}
    timestamp = _iso(data.get("timestamp"), unit="ms")

    records = []
    for asset_id, symbol in ids.items():
        coin = by_id[asset_id]
        records.append(
            _record(
                source="coincap",
                symbol=symbol,
                currency="USD",
                price=coin.get("priceUsd"),
                timestamp=timestamp,
                extra={
                    "id": coin.get("id"),
                    "name": coin.get("name"),
                    "rank": coin.get("rank"),
                    "market_cap_usd": coin.get("marketCapUsd"),
                    "volume_usd_24h": coin.get("volumeUsd24Hr"),
                    "change_percent_24h": coin.get("changePercent24Hr"),
                    "vwap_24h": coin.get("vwap24Hr"),
                    "supply": coin.get("supply"),
                    "max_supply": coin.get("maxSupply"),
                    "explorer": coin.get("explorer"),
                },
                raw=coin,
            )
        )
    return records


def fetch_coingecko(api_key, symbols=SYMBOLS, vs_currency="usd", timeout=DEFAULT_TIMEOUT):
    """CoinGecko simple price, one request for all coins.

    Demo key goes in the `x-cg-demo-api-key` header; the endpoint also works
    without a key, at a lower rate limit. CoinGecko addresses coins by slug
    (`binancecoin`, `ripple`), mapped from the symbol via COINS.
    """
    ids = _ids("coingecko", symbols)
    headers = {"accept": "application/json"}
    if api_key:
        headers["x-cg-demo-api-key"] = api_key

    data = _get(
        "https://api.coingecko.com/api/v3/simple/price",
        params={
            "ids": ",".join(ids),
            "vs_currencies": vs_currency,
            "include_market_cap": "true",
            "include_24hr_vol": "true",
            "include_24hr_change": "true",
            "include_last_updated_at": "true",
        },
        headers=headers,
        timeout=timeout,
    )

    records = []
    for coin_id, symbol in ids.items():
        if coin_id not in data:
            raise RuntimeError(f"coingecko error: no data for {coin_id!r}")
        coin = data[coin_id]
        records.append(
            _record(
                source="coingecko",
                symbol=symbol,
                currency=vs_currency.upper(),
                price=coin.get(vs_currency),
                timestamp=_iso(coin.get("last_updated_at")),
                extra={
                    "coin_id": coin_id,
                    "market_cap": coin.get(f"{vs_currency}_market_cap"),
                    "volume_24h": coin.get(f"{vs_currency}_24h_vol"),
                    "change_24h_percent": coin.get(f"{vs_currency}_24h_change"),
                },
                raw=coin,
            )
        )
    return records


def fetch_coinranking(api_key, symbols=SYMBOLS, timeout=DEFAULT_TIMEOUT):
    """Coinranking coins, one request for all coins.

    Key goes in the `x-access-token` header. Coinranking addresses coins by
    UUID, mapped from the symbol via COINS. The response carries no timestamp
    of its own, so prices are stamped with the time of the call.
    """
    ids = _ids("coinranking", symbols)
    data = _get(
        "https://api.coinranking.com/v2/coins",
        params=[("uuids[]", uuid) for uuid in ids],
        headers={"x-access-token": api_key},
        timeout=timeout,
    )
    if data.get("status") != "success":
        raise RuntimeError(f"coinranking error: {data}")

    by_uuid = {coin["uuid"]: coin for coin in data["data"]["coins"]}
    timestamp = _now_iso()

    records = []
    for uuid, symbol in ids.items():
        coin = by_uuid[uuid]
        records.append(
            _record(
                source="coinranking",
                symbol=symbol,
                currency="USD",
                price=coin.get("price"),
                timestamp=timestamp,
                extra={
                    "uuid": coin.get("uuid"),
                    "name": coin.get("name"),
                    "rank": coin.get("rank"),
                    "tier": coin.get("tier"),
                    "market_cap": coin.get("marketCap"),
                    "volume_24h": coin.get("24hVolume"),
                    "change_24h_percent": coin.get("change"),
                    "btc_price": coin.get("btcPrice"),
                    "all_time_high": coin.get("allTimeHigh"),
                    "sparkline_24h": coin.get("sparkline"),
                    "listed_at": _iso(coin.get("listedAt")),
                },
                raw=coin,
            )
        )
    return records


def fetch_coinpaprika(api_key=None, symbols=SYMBOLS, quote="USD", timeout=DEFAULT_TIMEOUT):
    """CoinPaprika tickers, one request per coin.

    The free tier needs no key, so `api_key` is optional; a paid key, if
    supplied, is sent as the `Authorization` header. CoinPaprika has no
    multi-coin ticker endpoint short of downloading every coin, so this
    makes one small request per coin instead.
    """
    ids = _ids("coinpaprika", symbols)
    headers = {"Accept": "application/json"}
    if api_key:
        headers["Authorization"] = api_key

    records = []
    for coin_id, symbol in ids.items():
        data = _get(
            f"https://api.coinpaprika.com/v1/tickers/{coin_id}",
            params={"quotes": quote},
            headers=headers,
            timeout=timeout,
        )
        quotes = data["quotes"][quote]
        records.append(
            _record(
                source="coinpaprika",
                symbol=symbol,
                currency=quote,
                price=quotes.get("price"),
                timestamp=data.get("last_updated"),
                extra={
                    "id": data.get("id"),
                    "name": data.get("name"),
                    "rank": data.get("rank"),
                    "market_cap": quotes.get("market_cap"),
                    "volume_24h": quotes.get("volume_24h"),
                    "volume_24h_change_24h": quotes.get("volume_24h_change_24h"),
                    "percent_change_1h": quotes.get("percent_change_1h"),
                    "percent_change_24h": quotes.get("percent_change_24h"),
                    "percent_change_7d": quotes.get("percent_change_7d"),
                    "ath_price": quotes.get("ath_price"),
                    "ath_date": quotes.get("ath_date"),
                    "circulating_supply": data.get("circulating_supply"),
                    "total_supply": data.get("total_supply"),
                    "max_supply": data.get("max_supply"),
                    "beta_value": data.get("beta_value"),
                },
                raw=data,
            )
        )
    return records


# --------------------------------------------------------------------------- #
# demo runner
# --------------------------------------------------------------------------- #
def _load_env(path=None):
    """Minimal .env reader, so the demo below needs no extra dependency."""
    path = path or os.path.join(os.path.dirname(_HERE), ".env")
    env = {}
    try:
        with open(path) as handle:
            for line in handle:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                env[key.strip()] = value.strip().strip("'\"")
    except FileNotFoundError:
        pass
    return env


if __name__ == "__main__":
    env = _load_env()
    providers = [
        ("coinlayer", fetch_coinlayer, env.get("COINLAYER")),
        ("coinmarketcap", fetch_coinmarketcap, env.get("COINMARKETCAP")),
        ("coincap", fetch_coincap, env.get("COINCAP")),
        ("coingecko", fetch_coingecko, env.get("COINGECKO")),
        ("coinranking", fetch_coinranking, env.get("COINRANKING")),
        ("coinpaprika", fetch_coinpaprika, env.get("COINPAPRIKA")),
    ]

    for name, fetch, key in providers:
        try:
            for record in fetch(key):
                print(f"{name:<15} {record['symbol']:<4} {record['price']:>13,.4f} {record['currency']} at {record['timestamp']}")
        except Exception as exc:  # noqa: BLE001 - demo script, show whatever broke
            print(f"{name:<15} failed: {type(exc).__name__}: {exc}")
        print()
