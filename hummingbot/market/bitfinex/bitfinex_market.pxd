from libcpp cimport bool
from hummingbot.market.market_base cimport MarketBase
from hummingbot.core.data_type.transaction_tracker cimport TransactionTracker


cdef class BitfinexMarket(MarketBase):
    # TODO: COIN-12, COIN-13, COIN-14
    cdef:
        object _ev_loop
        object _poll_notifier
        public bool _user_stream_tracker
        public object _bitfinex_auth
        list trading_pairs
        public object _user_stream_tracker_task
        TransactionTracker _tx_tracker

        double _last_timestamp
        double _last_order_update_timestamp
        double _poll_interval
        dict _in_flight_orders
        dict _trading_rules
        object _data_source_type
        object _coro_queue
        public object _status_polling_task
        public object _order_tracker_task
        public object _coro_scheduler_task
        public object _user_stream_event_listener_task
        public object _trading_rules_polling_task
        public object _shared_client