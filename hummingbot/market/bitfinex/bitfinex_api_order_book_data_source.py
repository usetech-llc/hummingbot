#!/usr/bin/env python
from collections import namedtuple
import logging
import time

import aiohttp
import asyncio
import ujson
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

from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.order_book_tracker_entry import (
    OrderBookTrackerEntry
)
from hummingbot.core.utils import async_ttl_cache
from hummingbot.logger import HummingbotLogger
from hummingbot.market.bitfinex.bitfinex_order_book import BitfinexOrderBook

BOOK_RET_TYPE = List[Dict[str, Any]]

BITFINEX_REST_URL = "https://api-pub.bitfinex.com/v2"
BITFINEX_WS_URI = "wss://api-pub.bitfinex.com/ws/2"

RESPONSE_SUCCESS = 200
NaN = float("nan")
MAIN_FIAT = "USD"

s_base, s_quote = slice(1, 4), slice(4, 7)

Ticker = namedtuple(
    "Ticker",
    "symbol bid bid_size ask ask_size daily_change daily_change_percent last_price volume high low"
)
BookStructure = namedtuple("Book", "order_id price amount")
TradeStructure = namedtuple("Trade", "id mts amount price")


class BitfinexAPIOrderBookDataSource(OrderBookTrackerDataSource):

    MESSAGE_TIMEOUT = 30.0
    STEP_TIME_SLEEP = 1.0
    REQUEST_TTL = 60 * 30
    TIME_SLEEP_BETWEEN_REQUESTS = 5.0
    CACHE_SIZE = 1

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
    def _convert_volume(raw_prices: Dict[str, Any]) -> BOOK_RET_TYPE:
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
    def _prepare_snapshot(pair: str, raw_snapshot: List[BookStructure]) -> Dict[str, Any]:
        bids = [
            {"price": i.price, "amount": i.amount, "orderId": i.order_id}
            for i in raw_snapshot if i.amount > 0
        ]
        asks = [
            {"price": i.price, "amount": abs(i.amount), "orderId": i.order_id}
            for i in raw_snapshot if i.amount < 0
        ]
        return {
            "symbol": pair,
            "bids": bids,
            "asks": asks,
        }

    async def get_snapshot(self, client: aiohttp.ClientSession, trading_pair: str) -> Dict[str, Any]:
        request_url: str = f"{BITFINEX_REST_URL}/book/t{trading_pair}/R0"

        async with client.get(request_url)as response:
            response: aiohttp.ClientResponse = response

            if response.status != RESPONSE_SUCCESS:
                raise IOError(f"Error fetching Coinbase Pro market snapshot for {trading_pair}. "
                              f"HTTP status is {response.status}.")

            raw_data: Dict[str, Any] = await response.json()
            return self._prepare_snapshot(trading_pair, [BookStructure(*i) for i in raw_data])

    async def get_tracking_pairs(self) -> Dict[str, OrderBookTrackerEntry]:
        result: Dict[str, OrderBookTrackerEntry] = {}

        trading_pairs: List[str] = await self.get_trading_pairs()
        number_of_pairs: int = len(trading_pairs)

        async with aiohttp.ClientSession() as client:
            for idx, trading_pair in enumerate(trading_pairs):
                try:
                    snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair)
                    snapshot_timestamp: float = time.time()
                    snapshot_msg: OrderBookMessage = BitfinexOrderBook.snapshot_message_from_exchange(
                        snapshot,
                        snapshot_timestamp,
                        metadata={"symbol": trading_pair}
                    )

                    order_book: OrderBook = self.order_book_create_function()
                    order_book.apply_snapshot(
                        snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id
                    )

                    result[trading_pair] = OrderBookTrackerEntry(
                        trading_pair, snapshot_timestamp, order_book
                    )
                    self.logger().info(
                        "Initialized order book for {trading_pair}. "
                        f"{idx+1}/{number_of_pairs} completed."
                    )
                    await asyncio.sleep(self.STEP_TIME_SLEEP)
                except IOError:
                    self.logger().network(
                        f"Error getting snapshot for {trading_pair}.",
                        exc_info=True,
                        app_warning_msg=f"Error getting snapshot for {trading_pair}. "
                                        "Check network connection."
                    )
                except Exception:
                    self.logger().error(
                        f"Error initializing order book for {trading_pair}. ",
                        exc_info=True
                    )

        return result

    def _prepare_trade(self, raw_response: str) -> Dict[str, Any]:
        _, content = ujson.loads(raw_response)
        trade = TradeStructure(*content)
        return {
            "id": trade.id,
            "mts": trade.mts,
            "amount": trade.amount,
            "price": trade.price,
        }

    async def _listen_trades_for_pair(self, pair: str, output: asyncio.Queue):
        while True:
            try:
                async with websockets.connect(BITFINEX_WS_URI) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    subscribe_request: Dict[str, Any] = {
                        "event": "subscribe",
                        "channel": "trades",
                        "symbol": f"t{pair}",
                    }
                    await ws.send(ujson.dumps(subscribe_request))
                    async for raw_msg in self._get_response(ws):
                        msg = self._prepare_trade(raw_msg)
                        msg_book: OrderBookMessage = BitfinexOrderBook.trade_message_from_exchange(
                            msg,
                            metadata={"symbol": f"t{pair}"}
                        )
                        output.put_nowait(msg_book)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error with WebSocket connection. "
                    f"Retrying after {int(self.MESSAGE_TIMEOUT)} seconds...",
                    exc_info=True)
                await asyncio.sleep(self.MESSAGE_TIMEOUT)

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        trading_pairs: List[str] = await self.get_trading_pairs()

        for pair in trading_pairs:
            asyncio.ensure_future(
                self._listen_trades_for_pair(pair, output)
            )

    async def _get_response(self, ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        try:
            await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)     # response
            await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)     # subscribe info
            await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)     # snapshot
            while True:
                try:
                    msg: str = await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)
                    yield msg
                except asyncio.TimeoutError:
                    raise
        except asyncio.TimeoutError:
            self.logger().warning("WebSocket ping timed out. Going to reconnect...")
            return
        except ConnectionClosed:
            return
        finally:
            await ws.close()

    def _prepare_response(self, raw_response: str) -> Dict[str, Any]:
        _, content = ujson.loads(raw_response)
        book = BookStructure(*content)
        return self._prepare_snapshot([book])

    async def _listen_order_book_for_pair(self, pair: str, output: asyncio.Queue):
        while True:
            try:
                async with websockets.connect(BITFINEX_WS_URI) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    subscribe_request: Dict[str, Any] = {
                        "event": "subscribe",
                        "channel": "book",
                        "prec": "R0",
                        "symbol": f"t{pair}",
                    }
                    await ws.send(ujson.dumps(subscribe_request))
                    async for raw_msg in self._get_response(ws):
                        msg = self._prepare_response(raw_msg)
                        msg_book: OrderBookMessage = BitfinexOrderBook.diff_message_from_exchange(msg)
                        output.put_nowait(msg_book)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    f"Unexpected error with WebSocket connection.",
                    exc_info=True,
                    app_warning_msg="Unexpected error with WebSocket connection. "
                                    f"Retrying in {int(self.MESSAGE_TIMEOUT)} seconds. "
                                    "Check network connection."
                )
                await asyncio.sleep(self.MESSAGE_TIMEOUT)

    async def listen_for_order_book_diffs(self,
                                          ev_loop: asyncio.BaseEventLoop,
                                          output: asyncio.Queue):
        trading_pairs: List[str] = await self.get_trading_pairs()

        for pair in trading_pairs:
            asyncio.ensure_future(
                self._listen_order_book_for_pair(pair, output)
            )

    async def listen_for_order_book_snapshots(self,
                                              ev_loop: asyncio.BaseEventLoop,
                                              output: asyncio.Queue):
        while True:
            trading_pairs: List[str] = await self.get_trading_pairs()

            try:
                async with aiohttp.ClientSession() as client:
                    for pair in trading_pairs:
                        try:
                            snapshot: Dict[str, Any] = await self.get_snapshot(client, pair)
                            snapshot_timestamp: float = time.time()
                            snapshot_msg: OrderBookMessage = BitfinexOrderBook.snapshot_message_from_exchange(
                                snapshot,
                                snapshot_timestamp,
                                metadata={"product_id": pair}
                            )

                            output.put_nowait(snapshot_msg)
                            self.logger().debug(f"Saved order book snapshot for {pair}")

                            await asyncio.sleep(self.TIME_SLEEP_BETWEEN_REQUESTS)
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            self.logger().network(
                                f"Unexpected error with WebSocket connection.",
                                exc_info=True,
                                app_warning_msg="Unexpected error with WebSocket connection. "
                                                f"Retrying in {self.TIME_SLEEP_BETWEEN_REQUESTS} sec."
                                                f"Check network connection."
                            )
                            await asyncio.sleep(self.TIME_SLEEP_BETWEEN_REQUESTS)
                    this_hour: pd.Timestamp = pd.Timestamp.utcnow().replace(
                        minute=0, second=0, microsecond=0
                    )
                    next_hour: pd.Timestamp = this_hour + pd.Timedelta(hours=1)
                    delta: float = next_hour.timestamp() - time.time()
                    await asyncio.sleep(delta)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error", exc_info=True)
                await asyncio.sleep(self.TIME_SLEEP_BETWEEN_REQUESTS)
