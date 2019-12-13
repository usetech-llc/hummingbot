import asyncio
import contextlib
import logging
import os
import sys
import time
import unittest
from decimal import Decimal
from os.path import join, realpath
from typing import (
    List
)

import conf
from hummingbot.core.clock import (
    Clock,
    ClockMode
)
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import (
    MarketEvent,
    OrderType,
    TradeType,
    TradeFee,
    BuyOrderCreatedEvent,
    SellOrderCreatedEvent,
    OrderCancelledEvent,
    BuyOrderCompletedEvent, OrderFilledEvent, SellOrderCompletedEvent)
from hummingbot.core.utils.async_utils import (
    safe_ensure_future,
    safe_gather,
)
from hummingbot.logger.struct_logger import METRICS_LOG_LEVEL
from hummingbot.market.bitfinex.bitfinex_market import BitfinexMarket

sys.path.insert(0, realpath(join(__file__, "../../../")))
logging.basicConfig(level=METRICS_LOG_LEVEL)


class BitfinexMarketUnitTest(unittest.TestCase):
    events: List[MarketEvent] = [
        MarketEvent.ReceivedAsset,
        MarketEvent.BuyOrderCompleted,
        MarketEvent.SellOrderCompleted,
        MarketEvent.WithdrawAsset,
        MarketEvent.OrderFilled,
        MarketEvent.OrderCancelled,
        MarketEvent.TransactionFailure,
        MarketEvent.BuyOrderCreated,
        MarketEvent.SellOrderCreated,
        MarketEvent.OrderCancelled
    ]

    market: BitfinexMarket
    market_logger: EventLogger
    stack: contextlib.ExitStack

    @classmethod
    def setUpClass(cls):
        cls.clock: Clock = Clock(ClockMode.REALTIME)
        cls.market: BitfinexMarket = BitfinexMarket(
            conf.bitfinex_api_key,
            conf.bitfinex_secret_key,
            trading_pairs=["BTCUSD", "ETHUSD"]
        )
        print("Initializing Bitfinex market... this will take about a minute.")
        cls.ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        cls.clock.add_iterator(cls.market)
        cls.stack = contextlib.ExitStack()
        cls._clock = cls.stack.enter_context(cls.clock)
        cls.ev_loop.run_until_complete(cls.wait_til_ready())
        print("Ready.")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.stack.close()

    @classmethod
    async def wait_til_ready(cls):
        while True:
            now = time.time()
            next_iteration = now // 1.0 + 1
            if cls.market.ready:
                break
            else:
                await cls._clock.run_til(next_iteration)
            await asyncio.sleep(1.0)

    def setUp(self):
        self.db_path: str = realpath(join(__file__, "../bitfinex_test.sqlite"))
        try:
            os.unlink(self.db_path)
        except FileNotFoundError:
            pass

        self.market_logger = EventLogger()
        for event_tag in self.events:
            self.market.add_listener(event_tag, self.market_logger)

    def tearDown(self):
        for event_tag in self.events:
            self.market.remove_listener(event_tag, self.market_logger)
        self.market_logger = None

    async def run_parallel_async(self, *tasks):
        future: asyncio.Future = safe_ensure_future(safe_gather(*tasks))
        while not future.done():
            now = time.time()
            next_iteration = now // 1.0 + 1
            await self.clock.run_til(next_iteration)
        return future.result()

    def run_parallel(self, *tasks):
        return self.ev_loop.run_until_complete(self.run_parallel_async(*tasks))

    def test_get_fee(self):
        limit_fee: TradeFee = self.market.get_fee("ETH", "USDC", OrderType.LIMIT,
                                                  TradeType.BUY, 1, 1)
        self.assertGreater(limit_fee.percent, 0)
        self.assertEqual(len(limit_fee.flat_fees), 0)
        market_fee: TradeFee = self.market.get_fee("ETH", "USDC", OrderType.MARKET,
                                                   TradeType.BUY, 1)
        self.assertGreater(market_fee.percent, 0)
        self.assertEqual(len(market_fee.flat_fees), 0)

    def test_minimum_order_size(self):
        amount = Decimal('0.001')
        quantized_amount = self.market.quantize_order_amount("ETHUSD", amount)
        self.assertEqual(quantized_amount, 0)

    def test_get_balance(self):
        balance = self.market.get_balance("ETH")
        print(balance)
        self.assertGreater(balance, 0.04)

    @unittest.skip("temporary")
    def test_limit_buy(self):
        trading_pair = "ETHUSD"
        amount: Decimal = Decimal("0.04")
        # quantized_amount: Decimal = self.market.quantize_order_amount(trading_pair, amount)
        current_ask_price: Decimal = self.market.get_price(trading_pair, False)
        ask_price: Decimal = current_ask_price - Decimal("0.08") * current_ask_price
        quantize_ask_price: Decimal = self.market.quantize_order_price(trading_pair,
                                                                       ask_price)

        order_id = self.market.buy(trading_pair, amount, OrderType.LIMIT,
                                   quantize_ask_price)

        print("")
        print(
            "------------------------------------------------- ORDER SENT -------------------------------------- ")
        print("")

        # Wait for order creation event
        self.run_parallel(self.market_logger.wait_for(BuyOrderCreatedEvent))

        print("")
        print(
            "------------------------------------------ ORDER PLACED IN THE EXCHANGE ------------------------- ")
        print("")

        # Cancel order. Automatically asserts that order is tracked
        print("order_id: ", order_id)
        self.market.cancel(trading_pair, order_id)

        [order_cancelled_event] = self.run_parallel(
            self.market_logger.wait_for(OrderCancelledEvent))
        self.assertEqual(order_cancelled_event.order_id, order_id)
        print(
            "------------------------------------------ ORDER CANCELLED ------------------------- ")

        #
        # self.assertGreater(self.market.get_balance("ETH"), 0.04)
        # trading_pair = "ETHUSD"
        # amount: Decimal = Decimal('0.04')
        # print(amount)
        # quantized_amount: Decimal = self.market.quantize_order_amount(trading_pair,
        #                                                               amount)
        #
        # current_bid_price: Decimal = self.market.get_price(trading_pair, True)
        # bid_price: Decimal = current_bid_price + Decimal('0.08') * current_bid_price
        # quantize_bid_price: Decimal = self.market.quantize_order_price(trading_pair,
        #                                                                bid_price)
        #
        # print("trading_pair, quantized_amount, OrderType.LIMIT, quantize_bid_price",
        #       trading_pair, quantized_amount, OrderType.LIMIT, quantize_bid_price)
        # order_id = self.market.buy(trading_pair,
        #                            quantized_amount,
        #                            OrderType.LIMIT,
        #                            quantize_bid_price,
        #                            )
        # [order_completed_event] = self.run_parallel(
        #     self.market_logger.wait_for(BuyOrderCompletedEvent))
        # order_completed_event: BuyOrderCompletedEvent = order_completed_event
        # trade_events: List[OrderFilledEvent] = [t for t in self.market_logger.event_log
        #                                         if isinstance(t, OrderFilledEvent)]
        # base_amount_traded: Decimal = sum(t.amount for t in trade_events)
        # quote_amount_traded: Decimal = sum(t.amount * t.price for t in trade_events)
        #
        # self.assertTrue([evt.order_type == OrderType.LIMIT for evt in trade_events])
        # self.assertEqual(order_id, order_completed_event.order_id)
        # self.assertAlmostEqual(quantized_amount,
        #                        order_completed_event.base_asset_amount)
        # self.assertEqual("LTC", order_completed_event.base_asset)
        # self.assertEqual("ETH", order_completed_event.quote_asset)
        # self.assertAlmostEqual(base_amount_traded,
        #                        order_completed_event.base_asset_amount)
        # self.assertAlmostEqual(quote_amount_traded,
        #                        order_completed_event.quote_asset_amount)
        # self.assertTrue(any([isinstance(event, BuyOrderCreatedEvent) and event.order_id == order_id
        #                      for event in self.market_logger.event_log]))
        # # Reset the logs
        self.market_logger.clear()

    @unittest.skip("temporary")
    def test_limit_sell(self):
        '''
        Placing limit orders
        Test that a limit sell order can be placed (far from best ask price) and canceled,
        without waiting for order completion
        '''
        trading_pair = "ETHUSD"
        amount: Decimal = Decimal("-0.04")
        # quantized_amount: Decimal = self.market.quantize_order_amount(trading_pair, amount)
        current_ask_price: Decimal = self.market.get_price(trading_pair, False)
        ask_price: Decimal = current_ask_price + Decimal("0.08") * current_ask_price
        quantize_ask_price: Decimal = self.market.quantize_order_price(trading_pair,
                                                                       ask_price)

        order_id = self.market.sell(trading_pair, amount, OrderType.LIMIT,
                                    quantize_ask_price)

        print("")
        print(
            "------------------------------------------------- ORDER SENT -------------------------------------- ")
        print("")

        # Wait for order creation event
        self.run_parallel(self.market_logger.wait_for(SellOrderCreatedEvent))

        print("")
        print(
            "------------------------------------------ ORDER PLACED IN THE EXCHANGE ------------------------- ")
        print("")

        # Cancel order. Automatically asserts that order is tracked
        print("order_id: ", order_id)

        self.market.cancel(trading_pair, order_id)

        print("")
        print(
            "------------------------------------------ ORDER CANCELATION STARTED ------------------------- ")
        print("")

        [order_cancelled_event] = self.run_parallel(
            self.market_logger.wait_for(OrderCancelledEvent))

        print("")
        print(
            "------------------------------------------ ORDER CANCELATION EVENT RECEIVED ------------------------- ")
        print("")

        self.assertEqual(order_cancelled_event.order_id, order_id)

        '''
        [order_completed_event] = self.run_parallel(self.market_logger.wait_for(SellOrderCompletedEvent))
        order_completed_event: SellOrderCompletedEvent = order_completed_event
        trade_events = [t for t in self.market_logger.event_log if isinstance(t, OrderFilledEvent)]
        base_amount_traded = sum(t.amount for t in trade_events)
        quote_amount_traded = sum(t.amount * t.price for t in trade_events)

        print("trade_events {} "
              "base_amount_traded {} "
              "quote_amount_traded {}".format(trade_events,
                                              base_amount_traded,
                                              quote_amount_traded))
        time.sleep(10)
        self.assertTrue([evt.order_type == OrderType.LIMIT for evt in trade_events])
        self.assertEqual(order_id, order_completed_event.order_id)
        self.assertAlmostEqual(quantized_amount, order_completed_event.base_asset_amount)
        self.assertEqual("ETH", order_completed_event.base_asset)
        self.assertEqual("USD", order_completed_event.quote_asset)
        self.assertAlmostEqual(base_amount_traded, order_completed_event.base_asset_amount)
        self.assertAlmostEqual(quote_amount_traded, order_completed_event.quote_asset_amount)
        self.assertTrue(any([isinstance(event, SellOrderCreatedEvent) and event.order_id == order_id
                             for event in self.market_logger.event_log]))
        '''
        # Reset the logs
        self.market_logger.clear()

    # @unittest.skip("temporary")
    def test_execute_limit_buy(self):
        trading_pair = "ETHUSD"
        amount: Decimal = Decimal("0.05")
        quantized_amount: Decimal = self.market.quantize_order_amount(trading_pair,
                                                                      amount)

        bid_entries = self.market.order_books[trading_pair].bid_entries()

        most_top_bid = next(bid_entries)
        print("most_top_bid", most_top_bid)
        bid_price: Decimal = Decimal(most_top_bid.price)
        quantize_bid_price: Decimal = \
            self.market.quantize_order_price(trading_pair, bid_price)

        # order_id = self.market.buy(trading_pair, amount, OrderType.LIMIT,
        #                            quantize_ask_price)

        print("")
        print(
            "------------------------------------------------- ORDER SENT -------------------------------------- ")
        print("")

        # Wait for order creation event
        # self.run_parallel(self.market_logger.wait_for(BuyOrderCreatedEvent))

        print("")
        print(
            "------------------------------------------ ORDER PLACED IN THE EXCHANGE ------------------------- ")
        print("")

        # Cancel order. Automatically asserts that order is tracked
        # print("order_id: ", order_id)
        # time.sleep(5)

        print("trading_pair, quantized_amount, OrderType.LIMIT, quantize_bid_price",
              trading_pair, quantized_amount, OrderType.LIMIT, quantize_bid_price)

        order_id = self.market.buy(trading_pair,
                                   quantized_amount,
                                   OrderType.LIMIT,
                                   quantize_bid_price,
                                   )

        # self.run_parallel(self.market_logger.wait_for(BuyOrderCreatedEvent))
        print("BuyOrderCreatedEvent event happened")
        [order_completed_event] = self.run_parallel(
            self.market_logger.wait_for(BuyOrderCompletedEvent))
        order_completed_event: BuyOrderCompletedEvent = order_completed_event
        trade_events: List[OrderFilledEvent] = [t for t in self.market_logger.event_log
                                                if isinstance(t, OrderFilledEvent)]
        base_amount_traded: Decimal = sum(t.amount for t in trade_events)
        quote_amount_traded: Decimal = sum(t.amount * t.price for t in trade_events)
        print("-----------------")
        print(base_amount_traded)
        print(quote_amount_traded)
        print(order_completed_event)
        print(order_id)

        self.assertTrue([evt.order_type == OrderType.LIMIT for evt in trade_events])
        self.assertEqual(order_id, order_completed_event.order_id)
        self.assertAlmostEqual(quantized_amount,
                               order_completed_event.base_asset_amount)
        self.assertEqual("USD", order_completed_event.base_asset)
        self.assertEqual("ETH", order_completed_event.quote_asset)
        self.assertAlmostEqual(base_amount_traded,
                               order_completed_event.base_asset_amount)
        self.assertAlmostEqual(quote_amount_traded,
                               order_completed_event.quote_asset_amount)
        self.assertTrue(any([isinstance(event, BuyOrderCreatedEvent) and event.order_id == order_id
                             for event in self.market_logger.event_log]))
        # Reset the logs
        self.market_logger.clear()

    @unittest.skip("temporary")
    def test_execute_limit_sell(self):
        trading_pair = "ETHUSD"
        amount: Decimal = Decimal(0.04)
        quantized_amount: Decimal = self.market.quantize_order_amount(trading_pair,
                                                                      amount)
        # current_ask_price: Decimal = self.market.get_price(trading_pair, False)
        # ask_price: Decimal = current_ask_price + Decimal("0.08") * current_ask_price
        # quantize_bid_price: Decimal = \
        #     self.market.quantize_order_price(trading_pair, ask_price)

        # order_id = self.market.sell(trading_pair, amount, OrderType.LIMIT, quantize_ask_price)
        ask_entries = self.market.order_books[trading_pair].ask_entries()
        # for b_entry in self.market.order_books[trading_pair].bid_entries():
        #     print(b_entry)
        most_top_ask = next(ask_entries)
        ask_price: Decimal = Decimal(most_top_ask.price)
        quantize_ask_price: Decimal = \
            self.market.quantize_order_price(trading_pair, ask_price)
        print("most_top_ask", most_top_ask, quantize_ask_price, ask_price)

        # order_id = self.market.buy(trading_pair, amount, OrderType.LIMIT,
        #                            quantize_ask_price)

        print("")
        print(
            "------------------------------------------------- ORDER SENT -------------------------------------- ")
        print("")

        # Wait for order creation event
        # self.run_parallel(self.market_logger.wait_for(BuyOrderCreatedEvent))

        print("trading_pair, quantized_amount, OrderType.LIMIT, quantize_bid_price",
              trading_pair, quantized_amount, OrderType.LIMIT, quantize_ask_price)
        order_id = self.market.sell(trading_pair,
                                    quantized_amount,
                                    OrderType.LIMIT,
                                    quantize_ask_price,
                                    )

        # print(self.market_logger._logged_events)
        # self.run_parallel(self.market_logger.wait_for(SellOrderCreatedEvent))

        print("")
        print(
            "------------------------------------------ ORDER PLACED IN THE EXCHANGE ------------------------- ")
        print("")

        [order_completed_event] = self.run_parallel(
            self.market_logger.wait_for(SellOrderCompletedEvent))

        print("")
        print(
            "------------------------------------------ ORDER COMPLETED ------------------------- ")
        print("")

        order_completed_event: SellOrderCompletedEvent = order_completed_event
        trade_events: List[OrderFilledEvent] = [t for t in self.market_logger.event_log
                                                if isinstance(t, OrderFilledEvent)]
        base_amount_traded: Decimal = sum(t.amount for t in trade_events)
        quote_amount_traded: Decimal = sum(t.amount * t.price for t in trade_events)
        print("-----------------")
        print(trade_events)
        print(base_amount_traded)
        print(quote_amount_traded)
        print(order_completed_event)
        print(order_id)

        self.assertTrue([evt.order_type == OrderType.LIMIT for evt in trade_events])
        self.assertEqual(order_id, order_completed_event.order_id)
        self.assertAlmostEqual(quantized_amount,
                               order_completed_event.base_asset_amount)
        self.assertEqual("ETH", order_completed_event.base_asset)
        self.assertEqual("USD", order_completed_event.quote_asset)
        self.assertAlmostEqual(base_amount_traded,
                               order_completed_event.base_asset_amount)
        self.assertAlmostEqual(quote_amount_traded,
                               order_completed_event.quote_asset_amount)
        self.assertTrue(any([isinstance(event, SellOrderCreatedEvent) and event.order_id == order_id
                             for event in self.market_logger.event_log]))
        # Reset the logs
        self.market_logger.clear()
