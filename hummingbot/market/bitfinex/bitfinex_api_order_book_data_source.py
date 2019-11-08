#!/usr/bin/env python
from collections import namedtuple

import asyncio
import aiohttp
import json
import logging
import pandas as pd
from typing import (
    Any,
    AsyncIterable,
    Dict,
    List,
    Optional,
)
import websockets
from websockets.exceptions import ConnectionClosed

from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.utils import async_ttl_cache
from hummingbot.logger import HummingbotLogger

BITFINEX_REST_URL = "https://api-pub.bitfinex.com/v2"
BITFINEX_WS_URI = "wss://api-pub.bitfinex.com/ws/2"

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
BookStructure = namedtuple("BookStructure", "price count amount")


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
            raw_symbol = f"{price['base']}-{price['quote']}"
            symbol = f"{price['base']}{price['quote']}"
            prices.append(
                {
                    "symbol": symbol,
                    "volume": price["volume"] * price["price"]
                }
            )
            converters[price["base"]] = price["price"]
            del raw_prices[raw_symbol]

        for raw_symbol, item in raw_prices.items():
            symbol = f"{item['base']}{item['quote']}"
            if item["base"] in converters:
                prices.append(
                    {
                        "symbol": symbol,
                        "volume": item["volume"] * converters[item["base"]]
                    }
                )
                if item["quote"] not in converters:
                    converters[item["quote"]] = item["price"] / converters[item["base"]]
                continue

            if item["quote"] in converters:
                prices.append(
                    {
                        "symbol": symbol,
                        "volume": item["volume"] * item["price"] * converters[item["quote"]]
                    }
                )
                if item["base"] not in converters:
                    converters[item["base"]] = item["price"] * converters[item["quote"]]
                continue

            prices.append({"symbol": symbol, "volume": NaN})

        return prices

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

    async def get_trading_pairs(self) -> List[str]:
        """
        Get a list of active trading pairs
        (if the market class already specifies a list of trading pairs,
        returns that list instead of all active trading pairs)
        :returns: A list of trading pairs defined by the market class,
        or all active trading pairs from the rest API
        """
        if not self._symbols:
            try:
                active_markets: pd.DataFrame = await self.get_active_exchange_markets()
                self._symbols = active_markets.index.tolist()
            except Exception:
                msg = "Error getting active exchange information. Check network connection."
                self._symbols = []
                self.logger().network(
                    f"Error getting active exchange information.",
                    exc_info=True,
                    app_warning_msg=msg
                )

        return self._symbols

    @staticmethod
    async def _make_request(ws: websockets.WebSocketClientProtocol, request: dict) -> None:
        await ws.send(json.dumps(request))

    async def _get_response(self, ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        try:
            return await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)
        except asyncio.TimeoutError:
            self.logger().warning("WebSocket timed out. Going to reconnect...")
        except ConnectionClosed:
            self.logger().warning("Connection closed")

    async def _yield_response(self, ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        while True:
            yield await self._get_response(ws)

    @staticmethod
    def _get_snapshot(raw_snapshot: str) -> List[BookStructure]:
        ch_id, content = json.loads(raw_snapshot)
        return [BookStructure(*i) for i in content]

    @staticmethod
    def _get_diff(raw_diff: str) -> BookStructure:
        _, content = json.loads(raw_diff)
        return BookStructure(*content)

    def _apply_snapshot(self, raw_snapshot: str) -> None:
        _: List[BookStructure] = self._get_snapshot(raw_snapshot)

        # TODO: continue
        # snapshot: Dict[str, any] = await self.get_snapshot(client, trading_pair)
        # snapshot_timestamp: float = time.time()
        # snapshot_msg: OrderBookMessage = CoinbaseProOrderBook.snapshot_message_from_exchange(
        #     snapshot,
        #     snapshot_timestamp,
        #     metadata={"symbol": trading_pair}
        # )
        # order_book: OrderBook = self.order_book_create_function()
        # active_order_tracker: CoinbaseProActiveOrderTracker = CoinbaseProActiveOrderTracker()
        # bids, asks = active_order_tracker.convert_snapshot_message_to_order_book_row(snapshot_msg)
        # order_book.apply_snapshot(bids, asks, snapshot_msg.update_id)

    def _apply_diff(self, raw_diff) -> None:
        _: BookStructure = self._get_diff(raw_diff)

        # TODO: continue

    async def _listen_order_book_for_pair(self, pair: str):
        while True:
            ws: websockets.WebSocketClientProtocol = None
            try:
                async with websockets.connect(BITFINEX_WS_URI) as socket:
                    ws = socket
                    subscribe_request: Dict[str, Any] = {
                        "event": "subscribe",
                        "channel": "book",
                        "symbol": f"t{pair}",
                    }
                    await self._make_request(ws, subscribe_request)

                    raw_response = await self._get_response(ws)
                    self.logger().info(raw_response)

                    subscribe_info = await self._get_response(ws)
                    self.logger().info(subscribe_info)

                    raw_snapshot: str = await self._get_response(ws)
                    self._apply_snapshot(raw_snapshot)

                    async for diff in self._yield_response(ws):
                        self._apply_diff(diff)
            except asyncio.CancelledError:
                raise
            except Exception as err:
                self.logger().error(err)
                self.logger().error(
                    "Unexpected error with WebSocket connection. "
                    f"Retrying after {self.MESSAGE_TIMEOUT} seconds...",
                    exc_info=True
                )
                await asyncio.sleep(self.MESSAGE_TIMEOUT)
            finally:
                await ws.close()
