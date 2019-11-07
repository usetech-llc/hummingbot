#!/usr/bin/env python
from collections import namedtuple

import aiohttp
import logging
import pandas as pd
from typing import (
    List,
    Optional,
    Any,
    Dict,
)
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.utils import async_ttl_cache
from hummingbot.logger import HummingbotLogger

BITFINEX_REST_URL = "https://api-pub.bitfinex.com/v2"
# COINBASE_WS_FEED = "wss://ws-feed.pro.coinbase.com"
REQUEST_TTL = 60 * 30
CACHE_SIZE = 1
RESPONSE_SUCCESS = 200
MAX_RETRIES = 10
NaN = float("nan")

MAIN_FIAT = "USD"

s_base, s_quote = slice(1, 4), slice(4, 7)

Ticker = namedtuple(
    "Ticker",
    "symbol bid bid_size ask ask_size daily_change daily_change_percent last_price volume high low"
)


class BitfinexAPIOrderBookDataSource(OrderBookTrackerDataSource):

    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, symbols: Optional[List[str]] = None):
        super().__init__()
        self._symbols: Optional[List[str]] = symbols

    @staticmethod
    def _get_prices(data) -> Dict[str, Any]:
        pairs = [
            Ticker(*item)
            for item in data if item[0].startswith("t") and item[0].isalpha()
        ]

        return {
            f"{item.symbol[s_base]}-{item.symbol[s_quote]}": {
                "symbol": f"{item.symbol[s_base]}-{item.symbol[s_quote]}",
                "base": item.symbol[s_base],
                "quote": item.symbol[s_quote],
                "volume": item.volume,
                "price": item.last_price,
            }
            for item in pairs
        }

    @staticmethod
    def _convert_volume(raw_prices: Dict[str, Any]) -> List[Dict[str, Any]]:
        converters = {}
        prices = []

        for price in [v for v in raw_prices.values() if v["quote"] == MAIN_FIAT]:
            symbol = f"{price['base']}-{price['quote']}"
            prices.append(
                {
                    "symbol": symbol,
                    "volume": price["volume"] * price["price"]
                }
            )
            converters[price["base"]] = price["price"]
            del raw_prices[symbol]

        for symbol, item in raw_prices.items():
            if item["base"] in converters:
                prices.append(
                    {
                        "symbol": symbol,
                        "volume": item["volume"] * item["price"] * converters[item["base"]]
                    }
                )
                if item["quote"] not in converters:
                    converters[item["quote"]] = converters[item["base"]] * item["price"]
                continue

            if item["quote"] in converters:
                prices.append(
                    {
                        "symbol": symbol,
                        "volume": item["volume"] * (item["price"] / converters[item["quote"]])
                    }
                )
                if item["base"] not in converters:
                    converters[item["base"]] = item["price"] / converters[item["quote"]]
                continue

            prices.append({"symbol": symbol, "volume": NaN})

    @classmethod
    @async_ttl_cache(ttl=REQUEST_TTL, maxsize=CACHE_SIZE)
    async def get_active_exchange_markets(cls) -> pd.DataFrame:
        """
        *required
        Returns all currently active BTC trading pairs from Coinbase Pro,
        sorted by volume in descending order.
        """
        async with aiohttp.ClientSession() as client:
            async with client.get(f"{BITFINEX_REST_URL}/tickers?symbols=ALL") as tickers_response:
                tickers_response: aiohttp.ClientResponse = tickers_response

                status = tickers_response.status
                if status != RESPONSE_SUCCESS:
                    raise IOError(
                        f"Error fetching active Coinbase Pro markets. HTTP status is {status}.")

                data = await tickers_response.json()

                raw_prices = cls._get_prices(data)
                prices = cls._convert_volume(raw_prices)

                all_markets: pd.DataFrame = pd.DataFrame.from_records(data=prices, index="symbol")

                return all_markets.sort_values("volume", ascending=False)

    # TODO: make other methods
