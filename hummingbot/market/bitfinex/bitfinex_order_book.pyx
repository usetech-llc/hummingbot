import logging
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy.engine import RowProxy
import ujson

from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.core.data_type.order_book_message import (
    BitfinexOrderBookMessage,
    OrderBookMessage,
    OrderBookMessageType,
)
from hummingbot.logger import HummingbotLogger


_logger = None


cdef class BitfinexOrderBook(OrderBook):

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global _logger

        if _logger is None:
            _logger = logging.getLogger(__name__)

        return _logger

    @classmethod
    def snapshot_message_from_exchange(cls,
                                       msg: Dict[str, Any],
                                       timestamp: float,
                                       metadata: Optional[Dict] = None) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)
        return BitfinexOrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content=msg,
            timestamp=timestamp
        )

    @classmethod
    def diff_message_from_exchange(cls,
                                   msg: Dict[str, any],
                                   timestamp: Optional[float] = None,
                                   metadata: Optional[Dict] = None) -> OrderBookMessage:
        if metadata:
            msg.update(metadata)
        if "time" in msg:
            msg_time = pd.Timestamp(msg["time"]).timestamp()
        return BitfinexOrderBookMessage(
            message_type=OrderBookMessageType.DIFF,
            content=msg,
            timestamp=timestamp or msg_time)

    @classmethod
    def snapshot_message_from_db(cls,
                                 record: RowProxy,
                                 metadata: Optional[Dict] = None) -> OrderBookMessage:
        msg = record.json if type(record.json)==dict else ujson.loads(record.json)
        return BitfinexOrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content=msg,
            timestamp=record.timestamp * 1e-3
        )

    @classmethod
    def diff_message_from_db(cls,
                             record: RowProxy,
                             metadata: Optional[Dict] = None) -> OrderBookMessage:
        return BitfinexOrderBookMessage(
            message_type=OrderBookMessageType.DIFF,
            content=record.json,
            timestamp=record.timestamp * 1e-3
        )

    # @classmethod
    # def trade_receive_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None):
    #     """
    #     *used for backtesting
    #     Convert a row of trade data into standard OrderBookMessage format
    #     :param record: a row of trade data from the database
    #     :return: CoinbaseProOrderBookMessage
    #     """
    #     return CoinbaseProOrderBookMessage(
    #         OrderBookMessageType.TRADE,
    #         record.json,
    #         timestamp=record.timestamp * 1e-3
    #     )

    @classmethod
    def from_snapshot(cls, snapshot: OrderBookMessage):
        raise NotImplementedError("Bitfinex order book needs to retain individual order data.")

    @classmethod
    def restore_from_snapshot_and_diffs(self, snapshot: OrderBookMessage, diffs: List[OrderBookMessage]):
        raise NotImplementedError("Bitfinex order book needs to retain individual order data.")
