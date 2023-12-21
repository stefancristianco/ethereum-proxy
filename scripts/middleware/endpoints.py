import logging
import aiohttp
import asyncio
import os

from contextlib import suppress
from aiohttp import web

from middleware.abstract.component_base import ComponentBase
from middleware.message import Message

from middleware.message import check_for_error_message
from middleware.helpers import unreachable_code, suppress_decorator

#
# Setup logger
#

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

logger.setLevel(os.environ.setdefault("LOG_LEVEL", "INFO"))


class JsonRpcEndpoint(ComponentBase):
    class WsClosedException(Exception):
        pass

    def __init__(self, alias: str, config: dict):
        assert "tasks_count" in config
        assert "max_response_size" in config
        assert "response_timeout" in config
        assert "check_ssl" in config
        assert "max_queue_size" in config
        assert "url" in config

        super().__init__(alias, config)

        self.__queue = asyncio.Queue(self.config["max_queue_size"])

    def __repr__(self) -> str:
        return f"JsonRpcEndpoint()"

    def _on_application_setup(self, app: web.Application):
        app.cleanup_ctx.append(self.__ctx_cleanup)

    async def send_request(self, request: Message) -> Message:
        for _ in range(self.config["tasks_count"] + 1):
            # Enhance the request with a Future to block on until a response is available
            request[self.private_key("response_future")] = asyncio.Future()
            try:
                await self.__queue.put(request)
                return check_for_error_message(
                    await request[self.private_key("response_future")]
                )
            except JsonRpcEndpoint.WsClosedException as ex:
                # Socket connection closed due to inactivity... retry
                logger.warning(str(ex))
        raise Exception("Max retries exceeded")

    async def __ctx_cleanup(self, _):
        tasks = []
        async with aiohttp.ClientSession() as session:
            for _ in range(self.config["tasks_count"]):
                tasks.append(
                    asyncio.create_task(
                        self.__request_resolver(session, self.config["url"])
                    )
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
            await request_resolver(session, url)

    @suppress_decorator
    async def __request_resolver_loop_http(
        self, session: aiohttp.ClientSession, url: str
    ):
        """Endless request processing loop for http urls"""
        while True:
            request: Message = await self.__queue.get()
            try:
                request[self.private_key("response_future")].set_result(
                    await self.__process_http_request(request, session, url)
                )
            except Exception as ex:
                request[self.private_key("response_future")].set_exception(ex)
                raise
            finally:
                self.__queue.task_done()

    @suppress_decorator
    async def __request_resolver_loop_ws(
        self, session: aiohttp.ClientSession, url: str
    ):
        """Endless request processing loop for ws urls"""
        async with session.ws_connect(
            url,
            max_msg_size=self.config["max_response_size"],
            verify_ssl=self.config["check_ssl"],
        ) as ws:
            while True:
                request: Message = await self.__queue.get()
                try:
                    request[self.private_key("response_future")].set_result(
                        await self.__process_ws_request(request, ws)
                    )
                except Exception as ex:
                    request[self.private_key("response_future")].set_exception(ex)
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
                verify_ssl=self.config["check_ssl"],
                timeout=self.config["response_timeout"],
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
            msg = await ws.receive(timeout=self.config["response_timeout"])
        except asyncio.TimeoutError:
            # Enhance error message
            raise Exception("Timeout waiting for response")
        else:
            if msg.type == aiohttp.WSMsgType.CLOSED:
                raise JsonRpcEndpoint.WsClosedException(f"Ws request status: {msg}")
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
