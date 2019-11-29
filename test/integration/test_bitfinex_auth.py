import asyncio
import json
import unittest
from typing import List

import websockets

import conf
from market.bitfinex.bitfinex_api_order_book_data_source import BITFINEX_WS_AUTH_WS_URI
from market.bitfinex.bitfinex_auth import BitfinexAuth


class TestAuth(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()

        api_key = conf.bitfinex_api_key
        secret_key = conf.bitfinex_secret_key
        cls.auth = BitfinexAuth(api_key, secret_key)

    def test_auth(self):
        result: List[str] = self.ev_loop.run_until_complete(self.con_auth())
        print(result)
        assert "serverId" in result

    # def test_wallet(self):
    #     result: List[str] = self.ev_loop.run_until_complete(self.con_wallet())

    async def con_auth(self):
        async with websockets.connect(BITFINEX_WS_AUTH_WS_URI) as ws:
            ws: websockets.WebSocketClientProtocol = ws
            payload = self.auth.generate_auth_payload()
            await ws.send(json.dumps(payload))
            msg = await asyncio.wait_for(ws.recv(), timeout=30)  # response
            return msg

    # async def con_wallet(self):
    #     payload = {
    #         "event": 'subscribe',
    #         "channel": 'book',
    #         "prec": 'R0',
    #         "symbol": 'tETHUSD'
    #     }
    #     async with websockets.connect(BITFINEX_WS_AUTH_WS_URI) as ws:
    #         ws: websockets.WebSocketClientProtocol = ws
    #         auth_payload = self.auth.generate_auth_payload()
    #         await ws.send(json.dumps(auth_payload))
    #         aa = await ws.send(json.dumps(payload))
    #         print(aa)
    #         while True:
    #             msg = await asyncio.wait_for(ws.recv(), timeout=30)  # response
    #             print(msg)

    # out =  asyncio.Queue()
    # from market.bitfinex.bitfinex_api_user_stream_data_source import \
    #     BitfinexAPIUserStreamDataSource
    # busds = BitfinexAPIUserStreamDataSource(auth, [])
    # await busds.listen_for_user_stream(self.ev_loop, out)
    # {"event":"info","version":2,"serverId":"xxxxxx,"platform":{"status":1}}
    # {"event": "auth", "status": "OK", "chanId": 0, "userId": 678720,
    #  "auth_id": "xxxxxxx",
    #  "caps": {"orders": {"read": 1, "write": 0}, "account": {"read": 1, "write": 0},
    #           "funding": {"read": 1, "write": 0},
    #           "history": {"read": 1, "write": 0},
    #           "wallets": {"read": 1, "write": 0},
    #           "withdraw": {"read": 0, "write": 0},
    #           "positions": {"read": 1, "write": 0}}}
    # [0, "ps", []]
    # [0, "ws", []]
    # [0, "os", []]
    # [0, "fos", []]
    # [0, "fcs", []]
    # [0, "fls", []]
    # [0, "bu", [0, 0]]
    # [0, "hb"]
