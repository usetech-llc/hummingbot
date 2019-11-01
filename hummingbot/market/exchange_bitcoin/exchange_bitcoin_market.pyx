import aiohttp
import asyncio
from async_timeout import timeout
from decimal import Decimal
import json
import logging
import pandas as pd
import time


cdef class ExchangeBitcoinMarket(MarketBase):

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global s_logger
        if s_logger is None:
            s_logger = logging.getLogger(__name__)
        return s_logger

    cdef c_start_tracking_order(self,
                                str client_order_id,
                                str symbol,
                                object order_type,
                                object trade_type,
                                object price,
                                object amount):
        """
        Add new order to self._in_flight_orders mapping
        """
        raise NotImplementedError

    cdef c_stop_tracking_order(self, str order_id):
        """
        Delete an order from self._in_flight_orders mapping
        """
        raise NotImplementedError

    cdef c_did_timeout_tx(self, str tracking_id):
        """
        Triggers MarketEvent.TransactionFailure when an Ethereum transaction has timed out
        """
        raise NotImplementedError

