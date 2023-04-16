"""
Time based cache.
"""

import asyncio
import logging
import os

from aiohttp import web
from contextlib import suppress

from extensions.abstract.round_robin_selector import RoundRobinSelector
from utils.message import Message

from utils.helpers import get_or_default, is_response_success


#
# Setup logger
#

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

logger.setLevel(os.environ.setdefault("LOG_LEVEL", "INFO"))


MEDIUM_DURATION_FILTER = {
    # Ethereum API
    "eth_getBlockByHash",
    "eth_getBlockByNumber",
    "eth_getBlockTransactionCountByHash",
    "eth_getBlockTransactionCountByNumber",
    "eth_getLogs",
    # Trace API
    "trace_block",
}

LONG_DURATION_FILTER = {
    "eth_chainId",
    "net_listening",
    "net_peerCount",
    "net_version",
    "web3_clientVersion",
    "web3_sha3",
}

NO_CACHE_FILTER = {
    "eth_blockNumber",
    "eth_sendRawTransaction",
    "eth_subscribe",
    "eth_unsubscribe",
}


class Cache(RoundRobinSelector):
    def __init__(self, alias: str, config: dict):
        super().__init__(alias, config)

        self.__cache_duration = get_or_default(config, "cache_duration", 2)
        self.__medium_cache_duration = get_or_default(
            config, "medium_cache_duration", 30
        )
        self.__long_cache_duration = get_or_default(
            config, "long_cache_duration", 86400
        )

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
        return f"Cache({self.get_alias()}, {self.get_config()})"

    def __str__(self) -> str:
        return f"Cache({self.get_alias()})"

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

        if request in self.__hash_to_pending_response:
            """This request is already in progress.
            Must use the same result to reduce RPC calls.
            """
            self.__statistics_dict[method]["cache_hits"] += 1
            return await self.__hash_to_pending_response[request]

        """Future calls for the same information will block on this
        future until request is resolved.
        """
        self.__hash_to_pending_response[request] = asyncio.Future()
        try:
            # Retrieve response online
            response = await super()._handle_request(request)
            self.__add_response_to_cache(request, response)
            self.__hash_to_pending_response[request].set_result(response)
        except Exception as ex:
            # Make sure to unblock all pending tasks
            self.__hash_to_pending_response[request].set_exception(ex)

        try:
            # Avoid exception if this future is not awaited
            return await self.__hash_to_pending_response[request]
        finally:
            del self.__hash_to_pending_response[request]

    def _get_routes(self, prefix: str) -> list:
        return [web.post(f"{prefix}/statistics", self.__get_statistics)]

    async def __get_statistics(self, _: web.Request) -> web.Response:
        """Resolve '/cache/statistics' request"""
        return web.json_response(self.__statistics_dict)

    async def ctx_cleanup(self, _):
        tasks = [
            asyncio.create_task(
                self.__cache_cleaner(
                    self.__general_purpose_cache, self.__cache_duration
                )
            ),
            asyncio.create_task(
                self.__cache_cleaner(
                    self.__medium_duration_cache, self.__medium_cache_duration
                )
            ),
            asyncio.create_task(
                self.__cache_cleaner(
                    self.__long_duration_cache, self.__long_cache_duration
                )
            ),
        ]

        yield

        for task in tasks:
            task.cancel()
        with suppress(asyncio.CancelledError):
            await asyncio.gather(*tasks)

    async def __cache_cleaner(self, cache, duration: int):
        """Task in charge with removing old entries from the cache"""
        cache_snapshot = {}
        while True:
            await asyncio.sleep(duration)
            for msg_hash in cache_snapshot:
                del cache[msg_hash]
            cache_snapshot = dict(cache)

    def __add_response_to_cache(self, request: Message, response: Message):
        if is_response_success(response):
            request_obj = request.as_json()
            method = request_obj["method"]
            if not method in NO_CACHE_FILTER:
                if method in MEDIUM_DURATION_FILTER:
                    self.__medium_duration_cache[request] = response
                elif method in LONG_DURATION_FILTER:
                    self.__long_duration_cache[request] = response
                else:
                    self.__general_purpose_cache[request] = response

    def __retrive_response_from_cache(self, request: Message) -> Message:
        if request in self.__medium_duration_cache:
            return self.__medium_duration_cache[request]
        if request in self.__long_duration_cache:
            return self.__long_duration_cache[request]
        if request in self.__general_purpose_cache:
            return self.__general_purpose_cache[request]
