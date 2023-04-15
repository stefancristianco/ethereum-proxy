"""
RPC entrypoint provider for ethereum compatible chains.
"""

import logging
import aiohttp
import asyncio
import os

from aiohttp import web
from contextlib import suppress

from utils.message import Message
from extensions.abstract.round_robin_selector import RoundRobinSelector

from utils.message import make_response_from_exception
from utils.helpers import log_exception, get_or_default, unreachable_code

#
# Setup logger
#

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

logger.setLevel(os.environ.setdefault("LOG_LEVEL", "INFO"))


class Receiver(RoundRobinSelector):
    def __init__(self, alias: str, config: dict):
        super().__init__(alias, config)

        self.__max_request_size = get_or_default(config, "max_request_size", 10 * 1024)

        self.__ws_uuid = 0
        self.__active_ws_connections = {}

    def __repr__(self) -> str:
        return f"Receiver({self.get_alias()}, {self.get_config()})"

    def __str__(self) -> str:
        return f"Receiver({self.get_alias()})"

    async def on_shutdown(self, _):
        await asyncio.gather(
            *[
                self.__active_ws_connections[id].close()
                for id in self.__active_ws_connections
            ]
        )

    def _get_routes(self, prefix: str) -> list:
        logger.debug(f"{self}: {prefix}/")
        return [
            web.post(f"{prefix}/", self.__forward_request),
            web.get(f"{prefix}/ws", self.__ws_forward_request),
        ]

    async def __ws_forward_request(self, request: web.Request):
        """WS request handler"""
        local_ws_uuid = self.__ws_uuid
        self.__ws_uuid += 1

        ws = web.WebSocketResponse(max_msg_size=self.__max_request_size)
        await ws.prepare(request)

        self.__active_ws_connections[local_ws_uuid] = ws
        try:
            with suppress(ConnectionError):
                await self.__ws_forward_request_loop(ws)
        finally:
            del self.__active_ws_connections[local_ws_uuid]

        return ws

    async def __forward_request(self, request: web.Request):
        """HTTP request handler"""
        try:
            return self.__http_send_message(
                await super()._handle_request(Message(await request.read()))
            )
        except Exception as ex:
            log_exception(logger, ex)
            return self.__http_send_message(make_response_from_exception(ex))

    async def __ws_send_message(self, ws: web.WebSocketResponse, msg: Message):
        data = msg.as_raw_data()
        if isinstance(data, bytes):
            return await ws.send_bytes(data)
        if isinstance(data, str):
            return await ws.send_str(data)
        unreachable_code()

    def __http_send_message(self, msg: Message):
        return web.json_response(body=msg.as_raw_data())

    async def __ws_forward_request_loop(self, ws: web.WebSocketResponse):
        """Resolve a node request.
        The request is added to a queue and the call is suspended until
        the the request is resolved by a consumer tasks.
        """
        with suppress(ConnectionError):
            async for msg in ws:
                if (
                    msg.type != aiohttp.WSMsgType.BINARY
                    and msg.type != aiohttp.WSMsgType.TEXT
                ):
                    continue
                try:
                    await self.__ws_send_message(
                        ws, await super()._handle_request(Message(msg.data))
                    )
                except Exception as ex:
                    log_exception(logger, ex)
                    await self.__ws_send_message(ws, make_response_from_exception(ex))
