import asyncio
import collections
import json
import logging
import time
import traceback
from decimal import Decimal
from typing import Optional, List, Dict, Any

import aiohttp
from libc.stdint cimport int64_t
from libcpp cimport bool

import conf
from hummingbot.core.data_type.order_book cimport OrderBook
from hummingbot.core.data_type.order_book_tracker import OrderBookTrackerDataSourceType
from hummingbot.core.data_type.transaction_tracker import TransactionTracker
from hummingbot.core.event.events import (
    BuyOrderCreatedEvent,
    MarketEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderType,
    SellOrderCreatedEvent,
    TradeFee,
    TradeType,
)
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.logger import HummingbotLogger
from hummingbot.market.bitfinex import (
    BITFINEX_REST_AUTH_URL,
    BITFINEX_REST_URL,
)
from hummingbot.market.market_base import (
    MarketBase,
    OrderType,
)
from hummingbot.market.bitfinex.bitfinex_auth import BitfinexAuth
from hummingbot.market.bitfinex.bitfinex_in_flight_order import BitfinexInFlightOrder
from hummingbot.market.bitfinex.bitfinex_in_flight_order cimport BitfinexInFlightOrder
from hummingbot.market.bitfinex.bitfinex_order_book_tracker import \
    BitfinexOrderBookTracker
from hummingbot.market.bitfinex.bitfinex_user_stream_tracker import \
    BitfinexUserStreamTracker
from hummingbot.market.trading_rule cimport TradingRule

s_logger = None
s_decimal_0 = Decimal(0)
general_min_order_size = Decimal('0.05')
general_max_order_size = Decimal(1e9)
general_order_size_quantum = Decimal(conf.bitfinex_quote_increment)

Wallet = collections.namedtuple('Wallet',
                                'wallet_type currency balance unsettled_interest balance_available')


cdef class BitfinexMarketTransactionTracker(TransactionTracker):
    # TODO: COIN-12, COIN-13, COIN-14
    cdef:
        BitfinexMarket _owner

    def __init__(self, owner: BitfinexMarket):
        super().__init__()
        self._owner = owner

    cdef c_did_timeout_tx(self, str tx_id):
        TransactionTracker.c_did_timeout_tx(self, tx_id)
        self._owner.c_did_timeout_tx(tx_id)

cdef class BitfinexMarket(MarketBase):
    MARKET_RECEIVED_ASSET_EVENT_TAG = MarketEvent.ReceivedAsset.value
    MARKET_BUY_ORDER_COMPLETED_EVENT_TAG = MarketEvent.BuyOrderCompleted.value
    MARKET_SELL_ORDER_COMPLETED_EVENT_TAG = MarketEvent.SellOrderCompleted.value
    MARKET_WITHDRAW_ASSET_EVENT_TAG = MarketEvent.WithdrawAsset.value
    MARKET_ORDER_CANCELLED_EVENT_TAG = MarketEvent.OrderCancelled.value
    MARKET_TRANSACTION_FAILURE_EVENT_TAG = MarketEvent.TransactionFailure.value
    MARKET_ORDER_FAILURE_EVENT_TAG = MarketEvent.OrderFailure.value
    MARKET_ORDER_FILLED_EVENT_TAG = MarketEvent.OrderFilled.value
    MARKET_BUY_ORDER_CREATED_EVENT_TAG = MarketEvent.BuyOrderCreated.value
    MARKET_SELL_ORDER_CREATED_EVENT_TAG = MarketEvent.SellOrderCreated.value

    API_CALL_TIMEOUT = 10.0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global s_logger
        if s_logger is None:
            s_logger = logging.getLogger(__name__)
        return s_logger

    def __init__(self,
                 bitfinex_api_key: str,
                 bitfinex_secret_key: str,
                 poll_interval: float = 5.0,
                 # interval which the class periodically pulls status from the rest API
                 order_book_tracker_data_source_type: OrderBookTrackerDataSourceType =
                 OrderBookTrackerDataSourceType.EXCHANGE_API,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True):
        super().__init__()

        self._ev_loop = asyncio.get_event_loop()
        self._poll_notifier = asyncio.Event()

        self._trading_required = trading_required
        self._bitfinex_auth = BitfinexAuth(bitfinex_api_key, bitfinex_secret_key)
        self._order_book_tracker = BitfinexOrderBookTracker(
            data_source_type=order_book_tracker_data_source_type,
            trading_pairs=trading_pairs)
        self._user_stream_tracker = BitfinexUserStreamTracker(
            bitfinex_auth=self._bitfinex_auth, trading_pairs=trading_pairs)
        self._tx_tracker = BitfinexMarketTransactionTracker(self)

        self._last_timestamp = 0
        self._last_order_update_timestamp = 0
        self._poll_interval = poll_interval
        self._in_flight_orders = {}
        self._trading_rules = {}
        self._data_source_type = order_book_tracker_data_source_type
        self._status_polling_task = None
        self._order_tracker_task = None
        self._user_stream_tracker_task = None
        self._user_stream_event_listener_task = None
        self._trading_rules_polling_task = None
        self._shared_client = None

    @property
    def name(self) -> str:
        """
        *required
        :return: A lowercase name / id for the market. Must stay consistent with market name in global settings.
        """
        return "bitfinex"

    @property
    def bitfinex_auth(self) -> BitfinexAuth:
        """
        """
        return self._bitfinex_auth

    cdef c_tick(self, double timestamp):
        """
        *required
        Used by top level Clock to orchestrate components of the bot.
        This function is called frequently with every clock tick
        """
        cdef:
            int64_t last_tick = <int64_t> (self._last_timestamp / self._poll_interval)
            int64_t current_tick = <int64_t> (timestamp / self._poll_interval)

        MarketBase.c_tick(self, timestamp)
        if current_tick > last_tick:
            if not self._poll_notifier.is_set():
                self._poll_notifier.set()
        self._last_timestamp = timestamp

    @property
    def ready(self) -> bool:
        """
        *required
        :return: a boolean value that indicates if the market is ready for trading
        """
        return all(self.status_dict.values())

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        """
        *required
        Get mapping of all the order books that are being tracked.
        :return: Dict[trading_pair : OrderBook]
        """
        return self._order_book_tracker.order_books

    @property
    def status_dict(self) -> Dict[str]:
        """
        *required
        :return: a dictionary of relevant status checks.
        This is used by `ready` method below to determine if a market is ready for trading.
        """
        print("self._trading_required", self._trading_required)
        print("self._account_balances", self._account_balances)
        # print("self._trading_rules", self._trading_rules)
        # print("self._order_book_tracker.ready", self._order_book_tracker.ready)

        return {
            "order_books_initialized": self._order_book_tracker.ready,
            "account_balance": len(
                self._account_balances) > 0 if self._trading_required else True,
            "trading_rule_initialized": len(
                self._trading_rules) > 0 if self._trading_required else True
        }

    cdef object c_get_fee(self,
                          str base_currency,
                          str quote_currency,
                          object order_type,
                          object order_side,
                          object amount,
                          object price):
        """
        *required
        function to calculate fees for a particular order
        :returns: TradeFee class that includes fee percentage and flat fees
        """
        # There is no API for checking user's fee tier
        # Fee info from https://www.bitfinex.com/fees
        cdef:
            object maker_fee = Decimal("0.001")
            object taker_fee = Decimal("0.002")

        return TradeFee(
            percent=maker_fee if order_type is OrderType.LIMIT else taker_fee)

    async def _update_balances(self):
        """
        Pulls the API for updated balances
        """
        cdef:
            dict account_info
            list balances
            str asset_name
            set local_asset_names = set(self._account_balances.keys())
            set remote_asset_names = set()
            set asset_names_to_remove

        print("_update_balances",)

        account_balances = await self._api_balance()
        print("<<-- account_balances", account_balances)
        for balance_entry in account_balances:
            # TODO: need more info about other types: exchange, margin, funding
            if balance_entry.wallet_type != "exchange":
                continue
            asset_name = balance_entry.currency
            # None or 0
            available_balance = Decimal(balance_entry.balance_available or 0)
            total_balance = Decimal(balance_entry.balance or 0)
            self._account_available_balances[asset_name] = available_balance
            self._account_balances[asset_name] = total_balance
            remote_asset_names.add(asset_name)

        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    async def check_network(self) -> NetworkStatus:
        """
        *required
        Async function used by NetworkBase class to check if the market is online / offline.
        """
        print("check_network",)
        try:
            await self._api_platform_status()
        except asyncio.CancelledError:
            raise
        except Exception:
            print("check_network, EXp")
            print(traceback.print_exc())
            return NetworkStatus.NOT_CONNECTED
        print("check_network OK")
        return NetworkStatus.CONNECTED

    async def start_network(self):
        """
        *required
        Async function used by NetworkBase class to handle when a single market goes online
        """
        if self._order_tracker_task is not None:
            self._stop_network()
        print("start_network^^ self._trading_required", self._trading_required)
        self._order_tracker_task = safe_ensure_future(self._order_book_tracker.start())
        if self._trading_required:
            self._status_polling_task = safe_ensure_future(self._status_polling_loop())
            self._trading_rules_polling_task = safe_ensure_future(
                self._trading_rules_polling_loop())

    async def _status_polling_loop(self):
        """
        Background process that periodically pulls for changes from the rest API
        """
        while True:
            try:
                print("_status_polling_loop")
                self._poll_notifier = asyncio.Event()
                await self._poll_notifier.wait()

                print("_update_balances start")
                await safe_gather(
                    self._update_balances(),
                )
                print("_update_balances finished")
            except asyncio.CancelledError:
                raise
            except Exception:
                print(traceback.print_exc())
                self.logger().network(
                    "Unexpected error while fetching account updates.",
                    exc_info=True,
                    app_warning_msg=f"Could not fetch account updates on Bitfinex. "
                )

    async def _trading_rules_polling_loop(self):
        """
        Separate background process that periodically pulls for trading rule changes
        (Since trading rules don't get updated often, it is pulled less often.)
        """
        while True:
            try:
                await safe_gather(self._update_trading_rules())
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unexpected error while fetching trading rules.",
                    exc_info=True,
                    app_warning_msg=f"Could not fetch trading rule updates on Bitfinex. "
                                    f"Check network connection."
                )
                await asyncio.sleep(0.5)

    async def _api_balance(self):
        path_url = "auth/r/wallets"
        account_balances = await self._api_private("post", path_url=path_url, data={})
        print("-->account_balances", account_balances)
        wallets = []
        for balance_entry in account_balances:
            print(balance_entry[:5])
            wallets.append(Wallet._make(balance_entry[:5]))
        return wallets

    async def _api_platform_status(self):
        path_url = "platform/status"
        platform_status = await self._api_public("get", path_url=path_url)
        print("platform_status", platform_status)
        return platform_status

    async def _api_platform_config_pair_info(self):
        path_url = "conf/pub:info:pair"
        info = await self._api_public("get", path_url=path_url)
        print("platform_config_pair_info", info)
        return info[0] if len(info) > 0 else 0

    async def _api_public(self,
                          http_method: str,
                          path_url,
                          data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:

        url = f"{BITFINEX_REST_URL}/{path_url}"
        print("Fetching " + url)
        req = await self._api_do_request(http_method, url, None, data)
        return req

    async def _api_private(self,
                           http_method: str,
                           path_url,
                           data: Optional[Dict[Any]] = None) -> Dict[Any]:

        url = f"{BITFINEX_REST_AUTH_URL}/{path_url}"
        print("path_url", path_url)
        data_str = json.dumps(data)
        #  because BITFINEX_REST_AUTH_URL already have v2  postfix, but v2 need
        #  for generate right signature for path
        headers = self.bitfinex_auth.generate_api_headers(f"v2/{path_url}", data_str)
        print("requests.post('"+BITFINEX_REST_AUTH_URL + "/" +path_url +"'"+ ", headers=" + str(headers) + ", data=" + data_str + ", verify=True)")

        req = await self._api_do_request(http_method=http_method,
                                         url=url,
                                         headers=headers,
                                         data_str=data)
        print("--> req", req)
        return req

    async def _api_do_request(self,
                              http_method: str,
                              url,
                              headers,
                              data_str: Optional[str, list] = None) -> list:
        """
        A wrapper for submitting API requests to Coinbase Pro
        :returns: json data from the endpoints
        """

        client = await self._http_client()
        print("url, headers, ", url, headers, data_str)
        async with client.request(http_method,
                                  url=url, timeout=self.API_CALL_TIMEOUT, json=data_str,
                                  headers=headers) as response:
            data = await response.json()
            print("DATA", data)
            if response.status != 200:
                print(f"ERROR, status is not 200: {response.status}")
                raise IOError(
                    f"Error fetching data from {url}. HTTP status is {response.status}. {data}")
            return data

    async def _http_client(self) -> aiohttp.ClientSession:
        """
        :returns: Shared client session instance
        """
        if self._shared_client is None:
            self._shared_client = aiohttp.ClientSession()
        return self._shared_client

    cdef object c_get_order_size_quantum(self, str trading_pair, object order_size):
        """
        *required
        Get the minimum increment interval for order size (e.g. 0.01 USD)
        :return: Min order size increment in Decimal format
        """
        cdef:
            TradingRule trading_rule = self._trading_rules[trading_pair]

        # Coinbase Pro is using the min_order_size as max_precision
        # Order size must be a multiple of the min_order_size
        # print("self._trading_rules[trading_pair]", self._trading_rules, trading_pair)
        return trading_rule.min_order_size

    cdef object c_quantize_order_amount(self, str trading_pair, object amount, object price=s_decimal_0):
        """
        *required
        Note: Bitfinex does not provide API for correct order sizing. Hardcoded 0.05 minimum and 0.01 precision
        until better information is available.
        :return: Valid order amount in Decimal format
        """

        # global s_decimal_0
        # global general_min_order_size
        # global general_order_size_quantum
        # quantized_amount = (amount // general_order_size_quantum) * general_order_size_quantum
        #
        # print("calculate_quantized_amount", quantized_amount, amount, general_order_size_quantum, general_min_order_size)
        # # Check against min_order_size. If not passing either check, return 0.
        # if quantized_amount < general_min_order_size:
        #     return s_decimal_0
        #
        # # Check against max_order_size. If not passing either check, return 0.
        # if quantized_amount > general_max_order_size:
        #     return s_decimal_0
        #
        # print("quantized_amount-->", quantized_amount)
        # return quantized_amount
        cdef:
            TradingRule trading_rule = self._trading_rules[trading_pair]

        global s_decimal_0

        print("trading_pair, amount::", trading_pair, amount)
        quantized_amount = MarketBase.c_quantize_order_amount(self, trading_pair, amount)

        # Check against min_order_size. If not passing either check, return 0.
        if quantized_amount < trading_rule.min_order_size:
            return s_decimal_0

        # Check against max_order_size. If not passing either check, return 0.
        if quantized_amount > trading_rule.max_order_size:
            return s_decimal_0

        return quantized_amount

    cdef OrderBook c_get_order_book(self, str trading_pair):
        """
        :returns: OrderBook for a specific trading pair
        """
        cdef:
            dict order_books = self._order_book_tracker.order_books

        print(order_books)
        if trading_pair not in order_books:
            raise ValueError(f"No order book exists for '{trading_pair}'.")
        return order_books[trading_pair]

    cdef object c_get_order_price_quantum(self, str trading_pair, object price):
        """
        *required
        Get the minimum increment interval for price
        :return: Min order price increment in Decimal format
        """
        # print("self._trading_rules->", self._trading_rules)
        # print("self.trading_pair->", trading_pair)
        cdef:
            TradingRule trading_rule = self._trading_rules[trading_pair]
        return trading_rule.min_price_increment

    async def _update_trading_rules(self):
        """
        Pulls the API for trading rules (min / max order size, etc)
        """
        cdef:
            # The poll interval for withdraw rules is 60 seconds.
            int64_t last_tick = <int64_t> (self._last_timestamp / 60.0)
            int64_t current_tick = <int64_t> (self._current_timestamp / 60.0)

        if current_tick > last_tick or len(self._trading_rules) <= 0:
            info = await self._api_platform_config_pair_info()
            trading_rules_list = self._format_trading_rules(info)
            self._trading_rules.clear()
            for trading_rule in trading_rules_list:
                self._trading_rules[trading_rule.trading_pair] = trading_rule

    def _format_trading_rules(self, raw_trading_rules: List[Any]) -> List[TradingRule]:
        """
        Turns json data from API into TradingRule instances
        :returns: List of TradingRule
        """
        cdef:
            list retval = []
        for rule in raw_trading_rules:
            try:
                trading_pair_id = rule[0]
                retval.append(
                    TradingRule(trading_pair_id,
                                min_price_increment=Decimal(conf.bitfinex_quote_increment),
                                min_order_size=Decimal(str(rule[1][3])),
                                max_order_size=Decimal(str(rule[1][4]))))
            except Exception:
                self.logger().error(
                    f"Error parsing the trading_pair rule {rule}. Skipping.",
                    exc_info=True)
        return retval

    #  buy func
    async def place_order(self,
                          order_id: str,
                          trading_pair: str,
                          amount: Decimal,
                          is_buy: bool,
                          order_type: OrderType,
                          price: Decimal):
        """
        Async wrapper for placing orders through the rest API.
        :returns: json response from the API
        """
        path_url = "auth/w/order/submit"
        data = {
            "type": {
                OrderType.LIMIT.name: "EXCHANGE LIMIT",
                OrderType.MARKET.name: "MARKET",
            }[order_type.name],  # LIMIT, EXCHANGE
            "symbol": f't{trading_pair}',
            "price": str(price),
            "amount": str(amount),
            "flags": 0,
        }

        print("place_order->>", data)
        order_result = await self._api_private("post", path_url=path_url, data=data)
        return order_result

    cdef c_start_tracking_order(self,
                                str client_order_id,
                                str trading_pair,
                                object order_type,
                                object trade_type,
                                object price,
                                object amount):
        """
        Add new order to self._in_flight_orders mapping
        """
        self._in_flight_orders[client_order_id] = BitfinexInFlightOrder(
            client_order_id,
            None,
            trading_pair,
            order_type,
            trade_type,
            price,
            amount,
        )

    cdef str c_buy(self, str trading_pair, object amount,
                   object order_type=OrderType.MARKET, object price=s_decimal_0,
                   dict kwargs={}):
        """
        *required
        Synchronous wrapper that generates a client-side order ID and schedules the buy order.
        """
        cdef:
            int64_t tracking_nonce = <int64_t> (time.time() * 1e6)
            str order_id = str(f"buy-{trading_pair}-{tracking_nonce}")

        safe_ensure_future(
            self.execute_buy(order_id, trading_pair, amount, order_type, price))
        return order_id

    async def execute_buy(self,
                          order_id: str,
                          trading_pair: str,
                          amount: Decimal,
                          order_type: OrderType,
                          price: Optional[Decimal] = s_decimal_0):
        """
        Function that takes strategy inputs, auto corrects itself with trading rule,
        and submit an API request to place a buy order
        """
        cdef:
            TradingRule trading_rule = self._trading_rules[trading_pair]

        decimal_amount = self.quantize_order_amount(trading_pair, amount)
        decimal_price = self.quantize_order_price(trading_pair, price)
        if decimal_amount < trading_rule.min_order_size:
            raise ValueError(
                f"Buy order amount {decimal_amount} is lower than the minimum order size "
                f"{trading_rule.min_order_size}.")

        try:
            self.c_start_tracking_order(order_id, trading_pair, order_type,
                                        TradeType.BUY, decimal_price, decimal_amount)
            order_result = await self.place_order(order_id, trading_pair,
                                                  decimal_amount, True, order_type,
                                                  decimal_price)
            exchange_order_id = order_result["id"]
            tracked_order = self._in_flight_orders.get(order_id)
            if tracked_order is not None:
                self.logger().info(
                    f"Created {order_type} buy order {order_id} for {decimal_amount} {trading_pair}.")
                tracked_order.update_exchange_order_id(exchange_order_id)

            self.c_trigger_event(self.MARKET_BUY_ORDER_CREATED_EVENT_TAG,
                                 BuyOrderCreatedEvent(self._current_timestamp,
                                                      order_type,
                                                      trading_pair,
                                                      decimal_amount,
                                                      decimal_price,
                                                      order_id))
        except asyncio.CancelledError:
            raise
        except Exception:
            traceback.print_exc()
            self.c_stop_tracking_order(order_id)
            order_type_str = "MARKET" if order_type == OrderType.MARKET else "LIMIT"
            self.logger().network(
                f"Error submitting buy {order_type_str} order to Bitfinex for "
                f"{decimal_amount} {trading_pair} {price}.",
                exc_info=True,
                app_warning_msg="Failed to submit buy order to Bitfinex. "
                                "Check API key and network connection."
            )
            self.c_trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                 MarketOrderFailureEvent(self._current_timestamp,
                                                         order_id, order_type))

    cdef c_stop_tracking_order(self, str order_id):
        """
        Delete an order from self._in_flight_orders mapping
        """
        if order_id in self._in_flight_orders:
            del self._in_flight_orders[order_id]

    # sell
    cdef str c_sell(self,
                    str trading_pair,
                    object amount,
                    object order_type=OrderType.MARKET,
                    object price=s_decimal_0,
                    dict kwargs={}):
        """
        *required
        Synchronous wrapper that generates a client-side order ID and schedules the sell order.
        """
        cdef:
            int64_t tracking_nonce = <int64_t>(time.time() * 1e6)
            str order_id = str(f"sell-{trading_pair}-{tracking_nonce}")
        safe_ensure_future(self.execute_sell(order_id, trading_pair, amount, order_type, price))
        return order_id

    async def execute_sell(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           order_type: OrderType,
                           price: Optional[Decimal] = s_decimal_0):
        """
        Function that takes strategy inputs, auto corrects itself with trading rule,
        and submit an API request to place a sell order
        """
        cdef:
            TradingRule trading_rule = self._trading_rules[trading_pair]

        decimal_amount = self.quantize_order_amount(trading_pair, abs(amount))
        decimal_price = self.quantize_order_price(trading_pair, price)
        print("abs(decimal_amount)", decimal_amount)
        if decimal_amount < trading_rule.min_order_size:
            raise ValueError(f"Sell order amount {decimal_amount} is lower than the minimum order size "
                             f"{trading_rule.min_order_size}.")

        try:
            decimal_amount *= -1
            self.c_start_tracking_order(order_id, trading_pair, order_type, TradeType.SELL, decimal_price, decimal_amount)
            order_result = await self.place_order(order_id, trading_pair, decimal_amount, False, order_type, decimal_price)

            print("----------------------------- DEBUG 2 ------------------------ ")
            print(order_result)

            exchange_order_id = order_result[4][0]
            tracked_order = self._in_flight_orders.get(order_id)

            print("----------------------------- DEBUG 1 ------------------------ ")
            print(tracked_order)

            if tracked_order is not None:
                print("----------------------------- DEBUG 3 ------------------------ ")
                self.logger().info(f"Created {order_type} sell order {order_id} for {decimal_amount} {trading_pair}.")
                print("----------------------------- DEBUG 4 ------------------------ ")
                tracked_order.update_exchange_order_id(exchange_order_id)
                print("----------------------------- DEBUG 5 ------------------------ ")

            print("----------------------------- DEBUG 6 ------------------------ ")
            self.c_trigger_event(self.MARKET_SELL_ORDER_CREATED_EVENT_TAG,
                                 SellOrderCreatedEvent(self._current_timestamp,
                                                       order_type,
                                                       trading_pair,
                                                       decimal_amount,
                                                       decimal_price,
                                                       order_id))
            print("----------------------------- SellOrderCreatedEvent EVENT TRIGGERED ------------------------ ")

        except asyncio.CancelledError:
            raise
        except Exception:
            self.c_stop_tracking_order(order_id)
            order_type_str = "MARKET" if order_type == OrderType.MARKET else "LIMIT"
            self.logger().network(
                f"Error submitting sell {order_type_str} order to Bitfinex for "
                f"{decimal_amount} {trading_pair} {price}.",
                exc_info=True,
                app_warning_msg="Failed to submit sell order to Bitfinex. "
                                "Check API key and network connection."
            )
            self.c_trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                 MarketOrderFailureEvent(self._current_timestamp, order_id, order_type))

    async def execute_cancel(self, trading_pair: str, order_id: str):
        """
        Function that makes API request to cancel an active order
        """
        try:
            exchange_order_id = await self._in_flight_orders.get(order_id).get_exchange_order_id()
            path_url = "auth/w/order/cancel/{exchange_order_id}"

            data = {
                "id": exchange_order_id
            }

            print("cancel_order->>", data)
            cancel_result = await self._api_private("post", path_url=path_url, data=data)
            # return order_result
            print("------------------------------  cancel_result = " + cancel_result)

            return order_id

            '''
            cancelled_id = await self._api_request("delete", path_url=path_url)
            if cancelled_id == exchange_order_id:
                self.logger().info(f"Successfully cancelled order {order_id}.")
                self.c_stop_tracking_order(order_id)
                self.c_trigger_event(self.MARKET_ORDER_CANCELLED_EVENT_TAG,
                                     OrderCancelledEvent(self._current_timestamp, order_id))
                return order_id
            '''
        except IOError as e:
            if "order not found" in e.message:
                # The order was never there to begin with. So cancelling it is a no-op but semantically successful.
                self.logger().info(f"The order {order_id} does not exist on Coinbase Pro. No cancellation needed.")
                self.c_stop_tracking_order(order_id)
                self.c_trigger_event(self.MARKET_ORDER_CANCELLED_EVENT_TAG,
                                     OrderCancelledEvent(self._current_timestamp, order_id))
                return order_id
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().network(
                f"Failed to cancel order {order_id}: {str(e)}",
                exc_info=True,
                app_warning_msg=f"Failed to cancel the order {order_id} on Coinbase Pro. "
                                f"Check API key and network connection."
            )
        return None

    cdef c_cancel(self, str trading_pair, str order_id):
        """
        *required
        Synchronous wrapper that schedules cancelling an order.
        """
        safe_ensure_future(self.execute_cancel(trading_pair, order_id))
        return order_id
