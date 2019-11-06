#!/usr/bin/env python
from collections import namedtuple

import aiohttp
import logging
import pandas as pd
from typing import (
    List,
    Optional,
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
MAIN_FIATS = ["USD", "EUR", "GBP"]
MAIN_BASES = ["BTC", "ETH"]

s_base, s_quote = slice(3), slice(3, 6)

MAIN_PAIRS = [base + fiat for base in MAIN_BASES for fiat in MAIN_FIATS]

Ticker = namedtuple(
    "Ticker",
    "symbol bid bid_size ask ask_size daily_change daily_change_perc last_price volume high low"
)
Currency = namedtuple("Currency", "price volume")


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

                pairs = [
                    Ticker(*item)
                    for item in data if item[0].startswith("t") and item[0].isalpha()
                ]

                main_prices = {
                    item.symbol[1:]: Currency(item.last_price, item.volume)
                    for item in pairs if item.symbol[1:] in MAIN_PAIRS
                }

                currencies = {}

                for item in pairs:
                    if item.symbol[1:] in main_prices:
                        base = item.symbol[1:][s_base]
                        quote = item.symbol[1:][s_quote]
                        volume = item.volume
                        price = item.last_price
                    # TODO: elif

                    currencies[f"{base}-{quote}"] = volume * price

    # TODO: make other methods
