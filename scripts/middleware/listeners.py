import logging
import aiohttp
import asyncio
import os

from aiohttp import web
from contextlib import suppress

from middleware.message import Message
from middleware.abstract.component_base import ComponentBase

from middleware.helpers import (
    log_and_suppress_decorator,
    unreachable_code,
    get_or_default,
)
from middleware.message import make_response_from_exception

#
# Setup logger
#

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

logger.setLevel(os.environ.setdefault("LOG_LEVEL", "INFO"))


class WsListener(ComponentBase):
    """
    A component that listens for JSON requests over WS and forwards them to a specified callback.

    The `WsListener` class is a subclass of `ComponentBase` and provides functionality to wait for incoming JSON
    requests over WS. It acts as a listener and forwards the received requests to a callback function for further
    processing.

    Args:
        alias (str): An identifier or alias for the `WsListener` instance.
        config (dict): A dictionary containing configuration parameters for the listener.
        callback: A callback function that will be invoked when a JSON request is received.
    """

    def __init__(self, alias: str, config: dict, callback):
        config = {
            "max_request_size": get_or_default(
                config, "max_request_size", 10 * 1024  # 10KB
            )
        }
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

    @log_and_suppress_decorator
    async def __send_message(self, ws: web.WebSocketResponse, msg: Message):
        data = msg.as_raw_data()
        if isinstance(data, bytes):
            return await ws.send_bytes(data)
        if isinstance(data, str):
            return await ws.send_str(data)
        unreachable_code()

    async def __forward_request_loop(self, ws: web.WebSocketResponse):
        async for msg in ws:
            if (
                msg.type != aiohttp.WSMsgType.BINARY
                and msg.type != aiohttp.WSMsgType.TEXT
            ):
                continue
            try:
                await self.__send_message(ws, await self.__callback(Message(msg.data)))
            except Exception as ex:
                await self.__send_message(ws, make_response_from_exception(ex))


class HttpListener(ComponentBase):
    """
    A component that listens for JSON requests over HTTP and forwards them to a specified callback.

    The `HttpListener` class is a subclass of `ComponentBase` and provides functionality to wait for incoming JSON
    requests over HTTP. It acts as a listener and forwards the received requests to a callback function for further
    processing.

    Args:
        alias (str): An identifier or alias for the `HttpListener` instance.
        config (dict): A dictionary containing configuration parameters for the listener.
        callback: A callback function that will be invoked when a JSON request is received.
    """

    def __init__(self, alias: str, config: dict, callback):
        super().__init__(alias, config)

        self.__callback = callback

    def __repr__(self) -> str:
        return f"HttpListener()"

    async def handle_request(self, request: web.Request):
        """HTTP request handler"""
        try:
            return self.__http_send_message(
                await self.__callback(Message(await request.read()))
            )
        except Exception as ex:
            return self.__http_send_message(make_response_from_exception(ex))

    def _on_application_setup(self, _):
        pass

    @log_and_suppress_decorator(logger)
    def __http_send_message(self, msg: Message):
        return web.json_response(body=msg.as_raw_data())
