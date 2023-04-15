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

#
# Setup logger
#

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

logger.setLevel(os.environ.setdefault("LOG_LEVEL", "INFO"))

#
# Environment variables and global constants
#

# number of tasks per endpoint url
async_tasks_count = int(os.environ.setdefault("PROXY_MAIN_ASYNC_TASKS_COUNT", str(2)))
# max queue of request per pool url
max_queue_size = int(os.environ.setdefault("PROXY_MAIN_MAX_QUEUE_SIZE", str(100)))
# max size of a response (default: 100mb)
max_response_size = int(
    os.environ.setdefault("PROXY_MAIN_MAX_RESPONSE_SIZE", str(100 * 1024 * 1024))
)
# response timeout (default: 10s)
response_timeout = int(os.environ.setdefault("PROXY_MAIN_RESPONSE_TIMEOUT", str(10)))
# check ssl certificate
check_ssl = bool(int(os.environ.setdefault("PROXY_MAIN_RESPONSE_TIMEOUT", str(1))))

logger.info("========== Globals ===========")
logger.info(f"PROXY_MAIN_ASYNC_TASKS_COUNT: {async_tasks_count} tasks/endpoint")
logger.info(f"PROXY_MAIN_MAX_QUEUE_SIZE: {max_queue_size} entries/endpoint")
logger.info(f"PROXY_MAIN_MAX_RESPONSE_SIZE: {max_response_size} bytes")
logger.info(f"PROXY_MAIN_RESPONSE_TIMEOUT: {response_timeout} seconds")


class Endpoint(Extension):
    class WsClosedException(Exception):
        pass

    def __init__(self, url: str):
        super().__init__(url)

        self.__queue = asyncio.Queue(max_queue_size)

    def __repr__(self) -> str:
        return f"Endpoint({self.get_name()})"

    async def _handle_request(self, request: Message) -> Message:
        retries_left = async_tasks_count + 1
        while retries_left > 0:
            retries_left -= 1
            # Enhance the request with a Future to block on until a response is available
            request.attach("response_future", asyncio.Future())
            try:
                await self.__queue.put(request)
                return await request.retrieve("response_future")
            except Endpoint.WsClosedException as ex:
                # Socket connection closed due to inactivity... retry
                logger.warning(str(ex))
        raise Exception("Max retries exceeded")

    def _add_next_handler(self, _):
        # This step is final
        assert False

    async def ctx_cleanup(self, _):
        tasks = []
        async with aiohttp.ClientSession() as session:
            for _ in range(async_tasks_count):
                tasks.append(
                    asyncio.create_task(
                        self.__request_resolver(session, self.get_name())
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
            request = await self.__queue.get()
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
            url, max_msg_size=max_response_size, verify_ssl=check_ssl
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
                verify_ssl=check_ssl,
                raise_for_status=True,
                timeout=response_timeout,
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
            msg = await ws.receive(timeout=response_timeout)
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
