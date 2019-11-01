#!/usr/bin/env python

import asyncio
import bisect
from collections import (
    defaultdict,
    deque
)
import logging
import time
from typing import (
    Deque,
    Dict,
    List,
    Optional,
    Set
)

from hummingbot.core.event.events import TradeType
from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.order_book_tracker import OrderBookTracker, OrderBookTrackerDataSourceType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.market.exchange_bitcoin.exchange_bitcoin_api_order_book_data_source import ExchangeBitcoinAPIOrderBookDataSource
from hummingbot.core.data_type.order_book_message import (
    OrderBookMessageType,
    ExchangeBitcoinOrderBookMessage,
    OrderBookMessage)
from hummingbot.core.data_type.order_book_tracker_entry import ExchangeBitcoinOrderBookTrackerEntry
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.market.exchange_bitcoin.exchange_bitcoin_order_book import ExchangeBitcoinOrderBook
from hummingbot.market.exchange_bitcoin.exchange_bitcoin_active_order_tracker import ExchangeBitcoinActiveOrderTracker


class ExchangeBitcoinOrderBookTracker(OrderBookTracker):
    _cbpobt_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._cbpobt_logger is None:
            cls._cbpobt_logger = logging.getLogger(__name__)
        return cls._cbpobt_logger

    def data_source(self) -> OrderBookTrackerDataSource:
        raise NotImplementedError

    async def start(self):
        raise NotImplementedError

    async def stop(self):
        raise NotImplementedError
