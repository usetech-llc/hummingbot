import asyncio
import contextlib
import time
import unittest
from decimal import Decimal
from typing import Optional

import aiohttp

import conf

from hummingbot.core.clock import (
    Clock,
    ClockMode
)

from hummingbot.core.utils.async_utils import (
    safe_ensure_future,
    safe_gather,
)
from hummingbot.market.bitfinex.bitfinex_auth import BitfinexAuth
from hummingbot.market.bitfinex.bitfinex_market import BitfinexMarket
from hummingbot.market.bitfinex.bitfinex_user_stream_tracker import BitfinexUserStreamTracker


class BitfinexUserStreamTrackerUnitTest(unittest.TestCase):
    user_stream_tracker: Optional[BitfinexUserStreamTracker] = None

    market: BitfinexMarket
    stack: contextlib.ExitStack

    @classmethod
    def setUpClass(cls):
        cls._shared_client = None
        cls.ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        cls.bitfinex_auth = BitfinexAuth(conf.bitfinex_api_key,
                                         conf.bitfinex_secret_key)
        cls.trading_pairs = ["ETHUSD"]
        cls.user_stream_tracker: BitfinexUserStreamTracker = BitfinexUserStreamTracker(
            bitfinex_auth=cls.bitfinex_auth, trading_pairs=cls.trading_pairs)

        cls.clock: Clock = Clock(ClockMode.REALTIME)
        cls.market: BitfinexMarket = BitfinexMarket(
            conf.bitfinex_api_key,
            conf.bitfinex_secret_key,
            trading_pairs=["BTCUSD", "ETHUSD"]
        )
        print("Initializing Bitfinext market... this will take about a minute.")
        cls.clock.add_iterator(cls.market)
        cls.stack = contextlib.ExitStack()
        cls._clock = cls.stack.enter_context(cls.clock)
        cls.ev_loop.run_until_complete(cls.wait_til_ready())

    @classmethod
    async def _http_client(cls) -> aiohttp.ClientSession:
        """
        :returns: Shared client session instance
        """
        if cls._shared_client is None:
            cls._shared_client = aiohttp.ClientSession()
        return cls._shared_client

    @classmethod
    async def wait_til_ready(cls):
        while True:
            now = time.time()
            next_iteration = now // 1.0 + 1
            if cls.market.ready:
                break
            else:
                print("not ready")
                await cls._clock.run_til(next_iteration)
            await asyncio.sleep(1.0)

    async def run_parallel_async(self, *tasks):
        future: asyncio.Future = safe_ensure_future(safe_gather(*tasks))
        while not future.done():
            now = time.time()
            next_iteration = now // 1.0 + 1
            await self.clock.run_til(next_iteration)
        return future.result()

    def run_parallel(self, *tasks):
        return self.ev_loop.run_until_complete(self.run_parallel_async(*tasks))

    def test_balance_greater_than_zero(self):
        """
        This test should be run after the developer has implemented the limit buy and cancel
        in the corresponding market class
        """
        print("self.market.get_balance", self.market.get_balance("ETH"))
        self.assertGreater(self.market.get_balance("ETH"), Decimal("0"))
