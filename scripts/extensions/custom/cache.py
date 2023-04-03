"""
Time based cache.
"""

import asyncio
import logging
import os

from aiohttp import web
from typing import List
from contextlib import suppress

from extensions.round_robin_selector import RoundRobinSelector
from utils.message import Message


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

# Duration to keep entries in cache (in seconds)
cache_duration = int(os.environ.setdefault("PROXY_CACHE_DURATION", "2"))

logger.info("======== Cache Globals =======")
logger.info(f"PROXY_CACHE_DURATION: {cache_duration}")


class Cache(RoundRobinSelector):
    def __init__(self):
        super().__init__("Cache")

        self.__statistics_dict = {}
        self.__hash_to_pending_response = {}
        self.__hash_to_cached_response = {}

    def __repr__(self) -> str:
        return "Cache()"

    async def _handle_request(self, request: Message) -> Message:
        """Resolve the given request either from cache or online"""

        # Update statistics information
        request_obj = request.as_json()
        method = request_obj["method"]
        if not method in self.__statistics_dict:
            self.__statistics_dict[method] = {"calls": 0, "cache_hits": 0}
        self.__statistics_dict[method]["calls"] += 1

        # Resolve the request
        request_hash = hash(request)
        if request_hash in self.__hash_to_pending_response:
            """This request is already in progress.
            Must use the same result to reduce RPC calls.
            """
            self.__statistics_dict[method]["cache_hits"] += 1
            response = await self.__hash_to_pending_response[request_hash]
        elif request_hash in self.__hash_to_cached_response:
            """Retrieve response from cache"""
            self.__statistics_dict[method]["cache_hits"] += 1
            response = self.__hash_to_cached_response[request_hash]
        else:
            """Future calls for the same information will block on this
            future until request is resolved.
            """
            self.__hash_to_pending_response[request_hash] = asyncio.Future()
            try:
                # Retrieve response online
                response = await super()._handle_request(request)
                self.__hash_to_cached_response[request_hash] = response

                # Share response with all pending indentical requests
                self.__hash_to_pending_response[request_hash].set_result(response)
            except Exception as ex:
                # Make sure to unblock all pending tasks
                self.__hash_to_pending_response[request_hash].set_exception(ex)

            try:
                # Avoid exception if this future is not awaited
                response = await self.__hash_to_pending_response[request_hash]
            finally:
                del self.__hash_to_pending_response[request_hash]

        return response

    def _get_routes(self) -> List:
        return [("/cache/statistics", self.__get_statistics)]

    async def __get_statistics(self, request: web.Request) -> web.Response:
        """Resolve '/cache/statistics' request"""
        return web.json_response(self.__statistics_dict)

    async def _initialize(self) -> None:
        self.__cache_cleaner_task = asyncio.create_task(self.__cache_cleaner())
        await super()._initialize()

    async def _cancel(self) -> None:
        self.__cache_cleaner_task.cancel()
        with suppress(asyncio.CancelledError):
            await self.__cache_cleaner_task
        await super()._cancel()

    async def __cache_cleaner(self) -> None:
        """Task in charge with removing old entries from the cache"""
        hash_to_cached_response_snapshot = {}
        while True:
            await asyncio.sleep(cache_duration)

            for msg_hash in hash_to_cached_response_snapshot:
                del self.__hash_to_cached_response[msg_hash]
            hash_to_cached_response_snapshot = dict(self.__hash_to_cached_response)
