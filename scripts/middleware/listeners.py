import logging
import aiohttp
import asyncio
import os

from aiohttp import web
from contextlib import suppress

from middleware.message import Message
from middleware.abstract.component_base import ComponentBase

from middleware.helpers import log_and_suppress, unreachable_code

#
# Setup logger
#

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

logger.setLevel(os.environ.setdefault("LOG_LEVEL", "INFO"))


class WsListener(ComponentBase):
    def __init__(self, alias: str, config: dict, callback):
        assert "max_request_size" in config

        super().__init__(alias, config)

        self.__callback = callback
        self.__ws_uuid = 0
        self.__active_ws_connections = {}

    def __repr__(self) -> str:
        return "WsListener()"

    async def handle_request(self, request: web.Request):
        """WS request handler"""
        local_ws_uuid = self.__ws_uuid
        self.__ws_uuid += 1

        ws = web.WebSocketResponse(max_msg_size=self.config["max_request_size"])
        await ws.prepare(request)

        self.__active_ws_connections[local_ws_uuid] = ws
        try:
            with suppress(ConnectionError):
                await self.__forward_request_loop(ws)
        finally:
            del self.__active_ws_connections[local_ws_uuid]

        return ws

    def _on_application_setup(self, app: web.Application):
        app.on_shutdown.append(self.__on_shutdown)

    async def __on_shutdown(self, _):
        await asyncio.gather(
            *[
                self.__active_ws_connections[id].close()
                for id in self.__active_ws_connections
            ]
        )

    async def __send_message(self, ws: web.WebSocketResponse, msg: Message):
        data = msg.as_raw_data()
        if isinstance(data, bytes):
            return await ws.send_bytes(data)
        if isinstance(data, str):
            return await ws.send_str(data)
        unreachable_code()

    async def __forward_request_loop(self, ws: web.WebSocketResponse):
        """Resolve a node request.
        The request is added to a queue and the call is suspended until
        the the request is resolved by a consumer tasks.
        """
        async for msg in ws:
            if (
                msg.type != aiohttp.WSMsgType.BINARY
                and msg.type != aiohttp.WSMsgType.TEXT
            ):
                continue
            with log_and_suppress(logger, "ws-forward-request"):
                await self.__send_message(ws, await self.__callback(Message(msg.data)))


class HttpListener(ComponentBase):
    def __init__(self, alias: str, config: dict, callback):
        super().__init__(alias, config)

        self.__callback = callback

    def __repr__(self) -> str:
        return f"HttpListener()"

    async def handle_request(self, request: web.Request):
        """HTTP request handler"""
        with log_and_suppress(logger, "http-forward-request"):
            return self.__http_send_message(
                await self.__callback(Message(await request.read()))
            )

    def _on_application_setup(self, _):
        pass

    def __http_send_message(self, msg: Message):
        return web.json_response(body=msg.as_raw_data())
