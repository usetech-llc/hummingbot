from collections import defaultdict, deque
import logging
import time
from typing import Deque, Dict, List, Optional

import asyncio

from hummingbot.core.data_type.order_book_tracker import (
    OrderBookTracker,
    OrderBookTrackerDataSourceType,
)
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger

from .bitfinex_api_order_book_data_source import BitfinexAPIOrderBookDataSource

EXC_API = OrderBookTrackerDataSourceType.EXCHANGE_API
SAVED_MESSAGES_QUEUE_SIZE = 1000
CALC_STAT_MINUTE = 60.0

QUEUE_TYPE = Dict[str, Deque[OrderBookMessage]]


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
        # TODO: COIN-16: Implement BitfinexOrderBookTracker.start
        # self._refresh_tracking_tasks = safe_ensure_future(
        #     self._refresh_tracking_loop()
        # )
        # self._order_book_diff_router_task = safe_ensure_future(
        #     self._order_book_diff_router()
        # )
        # self._order_book_snapshot_router_task = safe_ensure_future(
        #     self._order_book_snapshot_router()
        # )

    async def _refresh_tracking_tasks(self):
        pass

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

    async def _order_book_snapshot_router(self):
        pass

    async def _track_single_book(self, symbol: str):
        pass
