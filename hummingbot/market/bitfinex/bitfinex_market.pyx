import asyncio
import collections
import json
import logging
import traceback
from decimal import Decimal
from typing import Optional, List, Dict, Any

import aiohttp
from libcpp cimport bool
from libc.stdint cimport int64_t

from hummingbot.core.data_type.order_book_tracker import OrderBookTrackerDataSourceType
from hummingbot.core.data_type.transaction_tracker import TransactionTracker
from hummingbot.core.event.events import MarketEvent
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.logger import HummingbotLogger
from hummingbot.market.bitfinex import (
    BITFINEX_REST_AUTH_URL,
    BITFINEX_REST_URL,
)
from hummingbot.market.bitfinex.bitfinex_auth import BitfinexAuth
from hummingbot.market.bitfinex.bitfinex_order_book_tracker import \
    BitfinexOrderBookTracker
from hummingbot.market.bitfinex.bitfinex_user_stream_tracker import \
    BitfinexUserStreamTracker


s_logger = None
s_decimal_0 = Decimal(0)


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
            int64_t last_tick = <int64_t>(self._last_timestamp / self._poll_interval)
            int64_t current_tick = <int64_t>(timestamp / self._poll_interval)

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
    def status_dict(self) -> Dict[str]:
        """
        *required
        :return: a dictionary of relevant status checks.
        This is used by `ready` method below to determine if a market is ready for trading.
        """
        print("self._trading_required", self._trading_required)
        print("self._account_balances", self._account_balances)

        return {
            # "order_books_initialized": self._order_book_tracker.ready,
            "account_balance": len(
                self._account_balances) > 0 if self._trading_required else True,
            # "trading_rule_initialized": len(self._trading_rules) > 0 if self._trading_required else True
        }

    async def _update_balances(self):
        """COINBASE_API_ENDPOINT
        Pulls the API for updated balancesCOINBASE_API_ENDPOINT
        """
        cdef:
            dict account_info
            list balances
            str asset_name
            set local_asset_names = set(self._account_balances.keys())
            set remote_asset_names = set()
            set asset_names_to_remove

        print("_update_balances", )

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
        print("check_network", )
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
        # self._order_tracker_task = safe_ensure_future(self._order_book_tracker.start())
        if self._trading_required:
            self._status_polling_task = safe_ensure_future(self._status_polling_loop())

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

    async def _api_balance(self):
        path_url = "v2/auth/r/wallets"
        Wallet = collections.namedtuple('Wallet',
                                        'wallet_type currency balance unsettled_interest balance_available')
        account_balances = await self._api_private("post", path_url=path_url, data={})
        print("-->account_balances", account_balances)
        wallets = []
        for balance_entry in account_balances:
            print(balance_entry[:5])
            wallets.append(Wallet._make(balance_entry[:5]))
        return wallets

    async def _api_platform_status(self):
        path_url = "v2/platform/status"
        platform_status = await self._api_public("get", path_url=path_url)
        print("platform_status", platform_status)
        return platform_status

    async def _api_public(self,
                          http_method: str,
                          path_url,
                          data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:

        url = f"{BITFINEX_REST_URL}{path_url}"
        req = await self._api_do_request(http_method, url, None, data)
        return req

    async def _api_private(self,
                           http_method: str,
                           path_url,
                           data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:

        url = f"{BITFINEX_REST_AUTH_URL}{path_url}"
        print("path_url", path_url)
        data_str = json.dumps(data)
        headers = self.bitfinex_auth.generate_api_headers(path_url, data_str)
        print("requests.post('"+BITFINEX_REST_AUTH_URL + path_url +"'"+ ", headers=" + str(headers) + ", data=" + data_str + ", verify=True)")

        print("headers", headers)
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
                                  url=url, timeout=self.API_CALL_TIMEOUT, json=data_str, headers=headers) as response:
            data = await response.json()
            print("DATA", data)
            if response.status != 200:
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
