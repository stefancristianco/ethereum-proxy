"""
Time based cache.
"""

import asyncio
import logging
import os

from aiohttp import web
from typing import List, Any
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
cache_duration = int(os.environ.setdefault("PROXY_CACHE_DURATION", str(2)))
medium_cache_duration = int(
    os.environ.setdefault("PROXY_CACHE_MEDIUM_DURATION", str(30))
)
long_cache_duration = int(
    os.environ.setdefault("PROXY_CACHE_LONG_DURATION", str(86400))
)

logger.info("======== Cache Globals =======")
logger.info(f"PROXY_CACHE_DURATION: {cache_duration} seconds")
logger.info(f"PROXY_CACHE_MEDIUM_DURATION: {medium_cache_duration} seconds")
logger.info(f"PROXY_CACHE_LONG_DURATION: {long_cache_duration} seconds")

MEDIUM_DURATION_FILTER = {
    "eth_getBlockByNumber",
    "eth_getLogs",
    "eth_getCode",
    "eth_getTransactionReceipt",
    "eth_getTransactionCount",
    "trace_block",
}

LONG_DURATION_FILTER = {"eth_chainId", "net_version", "web3_clientVersion"}

NO_CACHE_FILTER = {"eth_blockNumber"}


class Cache(RoundRobinSelector):
    def __init__(self):
        super().__init__("Cache")

        self.__statistics_dict = {}
        self.__hash_to_pending_response = {}

        """ Cache types:
        __general_purpose_cache: general purpose cache is used for all unclassified entries
        __medium_duration_cache: used for valid important responses to havy API calls (e.g. traces, logs)
        __long_duration_cache: used to store almost constant data (e.g. chain_id, net_version, etc)
        """
        self.__general_purpose_cache = {}
        self.__medium_duration_cache = {}
        self.__long_duration_cache = {}

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

        # Resolve the request, try cache first
        response = self.__retrive_response_from_cache(request)
        if response:
            self.__statistics_dict[method]["cache_hits"] += 1
            return response

        if hash(request) in self.__hash_to_pending_response:
            """This request is already in progress.
            Must use the same result to reduce RPC calls.
            """
            self.__statistics_dict[method]["cache_hits"] += 1
            return await self.__hash_to_pending_response[hash(request)]

        """Future calls for the same information will block on this
        future until request is resolved.
        """
        self.__hash_to_pending_response[hash(request)] = asyncio.Future()
        try:
            # Retrieve response online
            response = await super()._handle_request(request)
            self.__add_response_to_cache(request, response)
            self.__hash_to_pending_response[hash(request)].set_result(response)
        except Exception as ex:
            # Make sure to unblock all pending tasks
            self.__hash_to_pending_response[hash(request)].set_exception(ex)

        try:
            # Avoid exception if this future is not awaited
            return await self.__hash_to_pending_response[hash(request)]
        finally:
            del self.__hash_to_pending_response[hash(request)]

    def _get_routes(self) -> List:
        return [("/cache/statistics", self.__get_statistics)]

    async def __get_statistics(self, request: web.Request) -> web.Response:
        """Resolve '/cache/statistics' request"""
        return web.json_response(self.__statistics_dict)

    async def _initialize(self):
        self.__cache_cleaner_task = [
            asyncio.create_task(
                self.__cache_cleaner(self.__general_purpose_cache, cache_duration)
            ),
            asyncio.create_task(
                self.__cache_cleaner(
                    self.__medium_duration_cache, medium_cache_duration
                )
            ),
            asyncio.create_task(
                self.__cache_cleaner(self.__long_duration_cache, long_cache_duration)
            ),
        ]
        await super()._initialize()

    async def _cancel(self):
        for task in self.__cache_cleaner_task:
            task.cancel()
        with suppress(asyncio.CancelledError):
            await asyncio.gather(*self.__cache_cleaner_task)
        await super()._cancel()

    async def __cache_cleaner(self, cache: Any, duration: int):
        """Task in charge with removing old entries from the cache"""
        cache_snapshot = {}
        while True:
            await asyncio.sleep(duration)
            for msg_hash in cache_snapshot:
                del cache[msg_hash]
            cache_snapshot = dict(cache)

    def __is_message_valid(self, msg: Message) -> bool:
        """Sanity check the message and decide if it's worth caching it"""
        if len(msg) > 512:
            # Optimization: error messages are small in size
            return True
        msg_obj = msg.as_json()
        if not "result" in msg_obj or not msg_obj["result"]:
            return False
        return True

    def __add_response_to_cache(self, request: Message, response: Message):
        if self.__is_message_valid(response):
            request_obj = request.as_json()
            method = request_obj["method"]
            if not method in NO_CACHE_FILTER:
                if method in MEDIUM_DURATION_FILTER:
                    self.__medium_duration_cache[hash(request)] = response
                elif method in LONG_DURATION_FILTER:
                    self.__long_duration_cache[hash(request)] = response
                else:
                    self.__general_purpose_cache[hash(request)] = response

    def __retrive_response_from_cache(self, request: Message) -> Message:
        if hash(request) in self.__medium_duration_cache:
            return self.__medium_duration_cache[hash(request)]
        if hash(request) in self.__long_duration_cache:
            return self.__long_duration_cache[hash(request)]
        if hash(request) in self.__general_purpose_cache:
            return self.__general_purpose_cache[hash(request)]
