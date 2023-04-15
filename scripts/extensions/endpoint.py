"""
Endpoint connection.
"""

import logging
import aiohttp
import asyncio
import os

from contextlib import suppress

from utils.message import Message
from extensions.abstract.extension_base import Extension

from utils.helpers import get_or_default, unreachable_code

#
# Setup logger
#

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

logger.setLevel(os.environ.setdefault("LOG_LEVEL", "INFO"))


class Endpoint(Extension):
    class WsClosedException(Exception):
        pass

    def __init__(self, alias: str, config: dict):
        super().__init__(alias, config)

        self.__tasks_count = get_or_default(config, "tasks_count", 2)
        self.__max_queue_size = get_or_default(config, "max_queue_size", 100)
        self.__max_response_size = get_or_default(
            config, "max_response_size", 100 * 2**20  # 100MB
        )
        self.__response_timeout = get_or_default(config, "max_queue_size", 100)
        # check ssl certificate
        self.__check_ssl = get_or_default(config, "check_ssl", False)
        self.__queue = asyncio.Queue(self.__max_queue_size)

    def __repr__(self) -> str:
        return f"Endpoint({self.get_alias()}, {self.get_config()})"

    def __str__(self) -> str:
        return f"Endpoint({self.get_alias()})"

    async def _handle_request(self, request: Message) -> Message:
        for _ in range(self.__tasks_count + 1):
            # Enhance the request with a Future to block on until a response is available
            request.attach("response_future", asyncio.Future())
            try:
                await self.__queue.put(request)
                return await request.retrieve("response_future")
            except Endpoint.WsClosedException as ex:
                # Socket connection closed due to inactivity... retry
                logger.warning(str(ex))
        raise Exception("Max retries exceeded")

    async def ctx_cleanup(self, _):
        tasks = []
        async with aiohttp.ClientSession() as session:
            config = self.get_config()
            for _ in range(self.__tasks_count):
                tasks.append(
                    asyncio.create_task(self.__request_resolver(session, config["url"]))
                )

            yield

            for task in tasks:
                task.cancel()
            with suppress(asyncio.CancelledError):
                await asyncio.gather(*tasks)

    async def __request_resolver(self, session: aiohttp.ClientSession, url: str):
        """Delegate to the proper request resolver based on connection type"""
        request_resolver = self.__request_resolver_loop_http
        if url.startswith("ws"):
            request_resolver = self.__request_resolver_loop_ws

        while True:
            try:
                await request_resolver(session, url)
            except asyncio.CancelledError:
                # Allow this exception to break the loop during shutdown
                raise
            except:
                pass

    async def __request_resolver_loop_http(
        self, session: aiohttp.ClientSession, url: str
    ):
        """Endless request processing loop for http urls"""
        while True:
            request: Message = await self.__queue.get()
            try:
                request.retrieve("response_future").set_result(
                    await self.__process_http_request(request, session, url)
                )
            except Exception as ex:
                request.retrieve("response_future").set_exception(ex)
                raise
            finally:
                self.__queue.task_done()

    async def __request_resolver_loop_ws(
        self, session: aiohttp.ClientSession, url: str
    ):
        """Endless request processing loop for ws urls"""
        async with session.ws_connect(
            url, max_msg_size=self.__max_response_size, verify_ssl=self.__check_ssl
        ) as ws:
            while True:
                request: Message = await self.__queue.get()
                try:
                    request.retrieve("response_future").set_result(
                        await self.__process_ws_request(request, ws)
                    )
                except Exception as ex:
                    request.retrieve("response_future").set_exception(ex)
                    raise
                finally:
                    self.__queue.task_done()

    async def __process_http_request(
        self, request: Message, session: aiohttp.ClientSession, url: str
    ) -> Message:
        try:
            headers = {"content-type": "application/json"}
            async with session.post(
                url,
                headers=headers,
                data=request.as_raw_data(),
                verify_ssl=self.__check_ssl,
                timeout=self.__response_timeout,
                raise_for_status=True,
            ) as response:
                return Message(await response.read())
        except asyncio.TimeoutError:
            # Enhance error message
            raise Exception("Timeout waiting for response")

    async def __process_ws_request(
        self, request: Message, ws: aiohttp.ClientWebSocketResponse
    ) -> Message:
        await self.__ws_send_message(ws, request)
        try:
            msg = await ws.receive(timeout=self.__response_timeout)
        except asyncio.TimeoutError:
            # Enhance error message
            raise Exception("Timeout waiting for response")
        else:
            if msg.type == aiohttp.WSMsgType.CLOSED:
                raise Endpoint.WsClosedException(f"Ws request status: {msg}")
            if (
                msg.type != aiohttp.WSMsgType.BINARY
                and msg.type != aiohttp.WSMsgType.TEXT
            ):
                raise Exception(f"Ws request status: {msg}")
            return Message(msg.data)

    async def __ws_send_message(
        self, ws: aiohttp.ClientWebSocketResponse, msg: Message
    ):
        data = msg.as_raw_data()
        if isinstance(data, bytes):
            return await ws.send_bytes(data)
        if isinstance(data, str):
            return await ws.send_str(data)
        unreachable_code()
