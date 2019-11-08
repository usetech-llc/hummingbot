import logging
from typing import List, Optional

import asyncio

from hummingbot.core.data_type.order_book_tracker import (
    OrderBookTracker,
    OrderBookTrackerDataSourceType,
)
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.logger import HummingbotLogger

from .bitfinex_api_order_book_data_source import BitfinexAPIOrderBookDataSource

EXC_API = OrderBookTrackerDataSourceType.EXCHANGE_API


class BitfinexOrderBookTracker(OrderBookTracker):
    _logger: Optional[HummingbotLogger] = None

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
        self._symbols: Optional[List[str]] = symbols

    @property
    def exchange_name(self) -> str:
        return "bitfinex"

    @property
    def data_source(self) -> OrderBookTrackerDataSource:
        if not self._data_source:
            if self._data_source_type is EXC_API:
                self._data_source = BitfinexAPIOrderBookDataSource(symbols=self._symbols)
            else:
                raise ValueError(f"data_source_type {self._data_source_type} is not supported.")

        return self._data_source

    @data_source.setter
    def data_source(self, data_source):
        self._data_source = data_source

    async def start(self):
        await super().start()

        # TODO: COIN-16: Implement BitfinexOrderBookTracker.start
