BITFINEX_REST_URL = "https://api-pub.bitfinex.com/v2"
BITFINEX_REST_AUTH_URL = "https://api.bitfinex.com/v2"
BITFINEX_WS_URI = "wss://api-pub.bitfinex.com/ws/2"
BITFINEX_WS_AUTH_URI = "wss://api.bitfinex.com/ws/2"


class SubmitOrder:
    OID = 0

    def __init__(self, oid):
        self.oid = str(oid)

    @classmethod
    def parse(cls, order_snapshot):
        return cls(order_snapshot[cls.OID])
