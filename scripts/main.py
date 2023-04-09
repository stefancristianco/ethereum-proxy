#!/usr/bin/env python3

"""
Proxy for ethereum compatible chains.
"""

import gc
import logging
import aiohttp
import argparse
import asyncio
import os

from aiohttp import web
from contextlib import suppress

from utils.message import Message
from extensions.round_robin_selector import RoundRobinSelector
from extensions.extension_base import ExtensionBase

from extensions.extension_base import get_extension_by_name
from utils.message import make_response_from_exception
from utils.helpers import log_exception


#
# Read cmd line arguments
#

parser = argparse.ArgumentParser(
    prog="main.py",
    description="Starts a load balancer for ethereum compatible nodes",
    epilog="Work in progress, use at own risk!",
)


def add_multiarg_option(flag: str, required: bool, metavar: str, help: str):
    parser.add_argument(
        flag, required=required, metavar=metavar, type=str, nargs="+", help=help
    )


def add_boolean_option(flag: str, help: str):
    parser.add_argument(flag, action="store_true", help=help)


add_multiarg_option("--extensions", False, "EXTENSION", "list of extensions to use")
add_multiarg_option("--pool", True, "URL", "url address for end-point")

add_boolean_option("--check-ssl", "enable ssl certificate verification")

args = parser.parse_args()

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
# max size of a request (default: 10k)
max_request_size = int(
    os.environ.setdefault("PROXY_MAIN_MAX_REQUEST_SIZE", str(10 * 1024))
)
# max size of a response (default: 100mb)
max_response_size = int(
    os.environ.setdefault("PROXY_MAIN_MAX_RESPONSE_SIZE", str(100 * 1024 * 1024))
)
# response timeout (default: 10s)
response_timeout = int(os.environ.setdefault("PROXY_MAIN_RESPONSE_TIMEOUT", str(10)))

logger.info("========== Globals ===========")
logger.info(f"PROXY_MAIN_ASYNC_TASKS_COUNT: {async_tasks_count} tasks/endpoint")
logger.info(f"PROXY_MAIN_MAX_QUEUE_SIZE: {max_queue_size} entries/endpoint")
logger.info(f"PROXY_MAIN_MAX_REQUEST_SIZE: {max_request_size} bytes")
logger.info(f"PROXY_MAIN_MAX_RESPONSE_SIZE: {max_response_size} bytes")
logger.info(f"PROXY_MAIN_RESPONSE_TIMEOUT: {response_timeout} seconds")

#
# Load extensions
#

logger.info("====== Using Extensions ======")
logger.info(args.extensions)

extensions = []
if args.extensions:
    extensions = [get_extension_by_name(ext_name)() for ext_name in args.extensions]

#
# Initialize final endpoints
#

logger.info("======== Pool entries ========")
for url in args.pool:
    logger.info(url)

endpoint_queues = {
    url: {"queue": asyncio.Queue(max_queue_size), "task_count": 0} for url in args.pool
}

ws_uuid = 0
active_ws_connections = {}

#
# Proxy logic
#


class WsClosedException(Exception):
    pass


async def enter_pipeline(request: Message) -> Message:
    """Start request processing with first pipeline entry"""
    assert pipeline_head
    return await pipeline_head.do_handle_request(request)


async def on_shutdown(_: web.Application):
    logger.info("Shutdown was called")
    ws_closers = [active_ws_connections[id].close() for id in active_ws_connections]
    await asyncio.gather(*ws_closers)


async def ws_send_message(ws: web.WebSocketResponse, msg: Message):
    data = msg.as_raw_data()
    if isinstance(data, bytes):
        return await ws.send_bytes(data)
    if isinstance(data, str):
        return await ws.send_str(data)


def http_send_message(msg: Message):
    return web.json_response(body=msg.as_raw_data())


async def ws_forward_request_loop(ws: web.WebSocketResponse):
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
                await ws_send_message(ws, await enter_pipeline(Message(msg.data)))
            except Exception as ex:
                log_exception(logger, ex)
                await ws_send_message(ws, make_response_from_exception(ex))


async def ws_forward_request(request: web.Request):
    """WS request handler"""
    global ws_uuid
    local_ws_uuid = ws_uuid
    ws_uuid += 1

    ws = web.WebSocketResponse(max_msg_size=max_request_size)
    await ws.prepare(request)

    active_ws_connections[local_ws_uuid] = ws
    try:
        with suppress(ConnectionError):
            await ws_forward_request_loop(ws)
    finally:
        del active_ws_connections[local_ws_uuid]

    return ws


async def forward_request(request: web.Request):
    """HTTP request handler"""
    try:
        return http_send_message(await enter_pipeline(Message(await request.read())))
    except Exception as ex:
        log_exception(logger, ex)
        return http_send_message(make_response_from_exception(ex))


async def process_http_request(
    request: Message, session: aiohttp.ClientSession, url: str
) -> Message:
    try:
        async with session.post(
            url,
            data=request.as_raw_data(),
            verify_ssl=args.check_ssl,
            raise_for_status=True,
            timeout=response_timeout,
        ) as response:
            return Message(await response.read())
    except asyncio.TimeoutError:
        # Enhance error message
        raise Exception("Timeout waiting for response")


async def process_ws_request(
    request: Message, ws: aiohttp.ClientWebSocketResponse
) -> Message:
    await ws_send_message(ws, request)
    try:
        msg = await ws.receive(timeout=response_timeout)
    except asyncio.TimeoutError:
        # Enhance error message
        raise Exception("Timeout waiting for response")
    else:
        if msg.type == aiohttp.WSMsgType.CLOSED:
            raise WsClosedException(f"Ws request status: {msg}")
        if msg.type != aiohttp.WSMsgType.BINARY and msg.type != aiohttp.WSMsgType.TEXT:
            raise Exception(f"Ws request status: {msg}")
        return Message(msg.data)


async def request_resolver_loop_http(session: aiohttp.ClientSession, url: str):
    """Endless request processing loop for http urls"""
    request_queue = endpoint_queues[url]["queue"]
    while True:
        request = await request_queue.get()
        try:
            request.retrieve("response_future").set_result(
                await process_http_request(request, session, url)
            )
        except Exception as ex:
            request.retrieve("response_future").set_exception(ex)
            raise
        finally:
            request_queue.task_done()


async def request_resolver_loop_ws(session: aiohttp.ClientSession, url: str):
    """Endless request processing loop for ws urls"""
    request_queue = endpoint_queues[url]["queue"]
    async with session.ws_connect(
        url, max_msg_size=max_response_size, verify_ssl=args.check_ssl
    ) as ws:
        while True:
            request: Message = await request_queue.get()
            try:
                request.retrieve("response_future").set_result(
                    await process_ws_request(request, ws)
                )
            except Exception as ex:
                request.retrieve("response_future").set_exception(ex)
                raise
            finally:
                request_queue.task_done()


async def request_resolver(session: aiohttp.ClientSession, url: str):
    """Delegate to the proper request resolver based on connection type"""
    request_resolver = request_resolver_loop_http
    if url.startswith("ws"):
        request_resolver = request_resolver_loop_ws

    while True:
        try:
            await request_resolver(session, url)
        except asyncio.CancelledError:
            # Allow this exception to break the loop during shutdown
            raise
        except:
            pass


async def run_request_resolvers(app):
    """Run all request resolvers as background tasks.
    At most 100 tasks may be started to match the limit defined by default
    for an 'aiohttp.ClientSession' session instance.
    """
    tasks = []
    async with aiohttp.ClientSession() as session:
        for url in args.pool:
            for _ in range(async_tasks_count):
                endpoint_queues[url]["task_count"] += 1
                tasks.append(asyncio.create_task(request_resolver(session, url)))
        await pipeline_head.do_initialize()

        yield

        await pipeline_head.do_cancel()
        for task in tasks:
            task.cancel()
        with suppress(asyncio.CancelledError):
            await asyncio.gather(*tasks)


class Receiver(RoundRobinSelector):
    def __init__(self):
        super().__init__("receiver")

    async def _initialize(self):
        logger.info("Initialization started...")
        await super()._initialize()

    async def _cancel(self):
        logger.info("Cancel started...")
        await super()._cancel()

    def __repr__(self) -> str:
        return "Receiver()"


class Endpoint(ExtensionBase):
    def __init__(self, url: str):
        super().__init__(url)

    async def _handle_request(self, request: Message) -> Message:
        retries_left = endpoint_queues[self.get_name()]["task_count"] + 1
        while retries_left > 0:
            retries_left -= 1
            # Enhance the request with a Future to block on until a response is available
            request.attach("response_future", asyncio.Future())
            try:
                await endpoint_queues[self.get_name()]["queue"].put(request)
                return await request.retrieve("response_future")
            except WsClosedException as ex:
                # Socket connection closed due to inactivity... retry
                logger.warning(str(ex))
        raise Exception("Max retries exceeded")

    async def _initialize(self):
        logger.info("Initialization completed")

    async def _cancel(self):
        logger.info("Cancel completed")

    def __repr__(self) -> str:
        return f"Endpoint({self.get_name()})"


def main():
    app = web.Application()

    # Setup route handlers
    app.on_shutdown.append(on_shutdown)
    app.add_routes(
        [
            web.post("/", forward_request),
            web.get("/ws", ws_forward_request),
        ]
    )
    for ext in extensions:
        app.add_routes(
            [web.post(route, handler) for (route, handler) in ext.do_get_routes()]
        )

    # Build pipeline
    global pipeline_head
    pipeline_head = Receiver()

    prev_handler = pipeline_head
    for ext in extensions:
        prev_handler.do_add_next_handler(ext)
        prev_handler = ext
    for url in args.pool:
        final_handler = Endpoint(url)
        prev_handler.do_add_next_handler(final_handler)

    app.cleanup_ctx.append(run_request_resolvers)

    web.run_app(app=app)

    # Prevents exceptions during shutdown
    gc.collect()


if __name__ == "__main__":
    main()
