from collections import defaultdict, deque
import logging
import time
from typing import Deque, Dict, List, Optional, Set

import asyncio
import bisect

from hummingbot.core.data_type.order_book_message import (
    BitfinexOrderBookMessage,
    OrderBookMessageType,
    OrderBookMessage,
)
from hummingbot.core.data_type.order_book_tracker import (
    OrderBookTracker,
    OrderBookTrackerDataSourceType
)
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.order_book_tracker_entry import BitfinexOrderBookTrackerEntry
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger
from hummingbot.market.bitfinex.bitfinex_active_order_tracker import BitfinexActiveOrderTracker
from hummingbot.market.bitfinex.bitfinex_order_book import BitfinexOrderBook

from .bitfinex_api_order_book_data_source import BitfinexAPIOrderBookDataSource

EXC_API = OrderBookTrackerDataSourceType.EXCHANGE_API
SAVED_MESSAGES_QUEUE_SIZE = 1000
CALC_STAT_MINUTE = 60.0

QUEUE_TYPE = Dict[str, Deque[OrderBookMessage]]
TRACKER_TYPE = Dict[str, BitfinexOrderBookTrackerEntry]


class BitfinexOrderBookTracker(OrderBookTracker):
    _logger: Optional[HummingbotLogger] = None

    EXCEPTION_TIME_SLEEP = 5.0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self,
                 data_source_type: OrderBookTrackerDataSourceType = EXC_API,
                 symbols: Optional[List[str]] = None):
        super().__init__(data_source_type=data_source_type)
        self._order_book_diff_stream: asyncio.Queue = asyncio.Queue()
        self._order_book_snapshot_stream: asyncio.Queue = asyncio.Queue()

        self._ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        self._data_source: Optional[OrderBookTrackerDataSource] = None
        self._saved_message_queues: QUEUE_TYPE = defaultdict(
            lambda: deque(maxlen=SAVED_MESSAGES_QUEUE_SIZE)
        )
        self._symbols: Optional[List[str]] = symbols
        self._active_order_trackers: TRACKER_TYPE = defaultdict(BitfinexActiveOrderTracker)

    @property
    def data_source(self) -> OrderBookTrackerDataSource:
        if not self._data_source:
            if self._data_source_type is EXC_API:
                self._data_source = BitfinexAPIOrderBookDataSource(symbols=self._symbols)
            else:
                raise ValueError(f"data_source_type {self._data_source_type} is not supported.")

        return self._data_source

    @property
    def exchange_name(self) -> str:
        return "bitfinex"

    async def start(self):
        await super().start()

        self._order_book_trade_listener_task = safe_ensure_future(
            self.data_source.listen_for_trades(self._ev_loop, self._order_book_trade_stream)
        )
        self._order_book_diff_listener_task = safe_ensure_future(
            self.data_source.listen_for_order_book_diffs(self._ev_loop, self._order_book_diff_stream)
        )
        self._order_book_snapshot_listener_task = safe_ensure_future(
            self.data_source.listen_for_order_book_snapshots(
                self._ev_loop, self._order_book_snapshot_stream
            )
        )
        self._refresh_tracking_task = safe_ensure_future(
            self._refresh_tracking_loop()
        )
        self._order_book_diff_router_task = safe_ensure_future(
            self._order_book_diff_router()
        )
        self._order_book_snapshot_router_task = safe_ensure_future(
            self._order_book_snapshot_router()
        )

    async def _refresh_tracking_tasks(self):
        tracking_symbols: Set[str] = set(
            [
                key for key in self._tracking_tasks.keys()
                if not self._tracking_tasks[key].done()
            ]
        )

        available_pairs: TRACKER_TYPE = await self.data_source.get_tracking_pairs()
        available_symbols: Set[str] = set(available_pairs.keys())
        new_symbols: Set[str] = available_symbols - tracking_symbols
        deleted_symbols: Set[str] = tracking_symbols - available_symbols

        for symbol in new_symbols:
            order_book_tracker_entry: BitfinexOrderBookTrackerEntry = available_pairs[symbol]
            self._active_order_trackers[symbol] = order_book_tracker_entry.active_order_tracker
            self._order_books[symbol] = order_book_tracker_entry.order_book
            self._tracking_message_queues[symbol] = asyncio.Queue()
            self._tracking_tasks[symbol] = safe_ensure_future(self._track_single_book(symbol))
            self.logger().info("Started order book tracking for %s.", symbol)

        for symbol in deleted_symbols:
            self._tracking_tasks[symbol].cancel()
            del self._tracking_tasks[symbol]
            del self._order_books[symbol]
            del self._active_order_trackers[symbol]
            del self._tracking_message_queues[symbol]
            self.logger().info("Stopped order book tracking for %s.", symbol)

    async def _order_book_diff_router(self):
        last_message_timestamp: float = time.time()
        messages_queued: int = 0
        messages_accepted: int = 0
        messages_rejected: int = 0

        while True:
            try:
                order_book_message: OrderBookMessage = await self._order_book_diff_stream.get()
                symbol: str = order_book_message.symbol

                if symbol not in self._tracking_message_queues:
                    messages_queued += 1
                    # Save diff messages received before snapshots are ready
                    self._saved_message_queues[symbol].append(order_book_message)
                    continue

                message_queue: asyncio.Queue = self._tracking_message_queues[symbol]
                order_book: OrderBook = self._order_books[symbol]

                if order_book.snapshot_uid > order_book_message.update_id:
                    messages_rejected += 1
                    continue

                await message_queue.put(order_book_message)
                messages_accepted += 1

                # Log some statistics.
                now: float = time.time()
                if int(now / CALC_STAT_MINUTE) > int(last_message_timestamp / CALC_STAT_MINUTE):
                    self.logger().debug("Diff messages processed: %d, rejected: %d, queued: %d",
                                        messages_accepted,
                                        messages_rejected,
                                        messages_queued)
                    messages_accepted = 0
                    messages_rejected = 0
                    messages_queued = 0

                last_message_timestamp = now

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    f"Unexpected error routing order book messages.",
                    exc_info=True,
                    app_warning_msg="Unexpected error routing order book messages. "
                                    f"Retrying after {int(self.EXCEPTION_TIME_SLEEP)} seconds."
                )
                await asyncio.sleep(self.EXCEPTION_TIME_SLEEP)

    async def _track_single_book(self, symbol: str):
        past_diffs_window: Deque[BitfinexOrderBookMessage] = deque()
        self._past_diffs_windows[symbol] = past_diffs_window

        message_queue: asyncio.Queue = self._tracking_message_queues[symbol]
        order_book: BitfinexOrderBook = self._order_books[symbol]
        active_order_tracker: BitfinexActiveOrderTracker = self._active_order_trackers[symbol]

        last_message_timestamp: float = time.time()
        diff_messages_accepted: int = 0

        while True:
            try:
                saved_messages: Deque[BitfinexOrderBookMessage] = self._saved_message_queues[symbol]

                if len(saved_messages) > 0:
                    message = saved_messages.popleft()
                else:
                    message = await message_queue.get()

                if message.type is OrderBookMessageType.DIFF:
                    bids, asks = active_order_tracker.convert_diff_message_to_order_book_row(message)
                    order_book.apply_diffs(bids, asks, message.update_id)
                    past_diffs_window.append(message)

                    while len(past_diffs_window) > self.PAST_DIFF_WINDOW_SIZE:
                        past_diffs_window.popleft()
                    diff_messages_accepted += 1

                    # Output some statistics periodically.
                    now: float = time.time()
                    if int(now / CALC_STAT_MINUTE) > int(last_message_timestamp / CALC_STAT_MINUTE):
                        self.logger().debug(
                            "Processed %d order book diffs for %s.", diff_messages_accepted, symbol)
                        diff_messages_accepted = 0

                    last_message_timestamp = now
                elif message.type is OrderBookMessageType.SNAPSHOT:
                    past_diffs: List[BitfinexOrderBookMessage] = list(past_diffs_window)

                    # only replay diffs later than snapshot,
                    # first update active order with snapshot then replay diffs
                    replay_position = bisect.bisect_right(past_diffs, message)
                    replay_diffs = past_diffs[replay_position:]
                    s_bids, s_asks = active_order_tracker.convert_snapshot_message_to_order_book_row(
                        message
                    )
                    order_book.apply_snapshot(s_bids, s_asks, message.update_id)
                    for diff_message in replay_diffs:
                        d_bids, d_asks = active_order_tracker.convert_diff_message_to_order_book_row(
                            diff_message
                        )
                        order_book.apply_diffs(d_bids, d_asks, diff_message.update_id)

                    self.logger().debug("Processed order book snapshot for %s.", symbol)
            except asyncio.CancelledError:
                raise
            except Exception as err:
                self.logger().error("track single book", err)
                self.logger().network(
                    f"Unexpected error processing order book messages for {symbol}.",
                    exc_info=True,
                    app_warning_msg="Unexpected error processing order book messages. "
                                    f"Retrying after {int(self.EXCEPTION_TIME_SLEEP)} seconds."
                )
                await asyncio.sleep(self.EXCEPTION_TIME_SLEEP)
