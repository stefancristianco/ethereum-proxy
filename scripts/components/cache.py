"""
Time based cache.
"""

import asyncio
import logging
import os

from aiohttp import web
from contextlib import suppress

from components.abstract.round_robin_selector import RoundRobinSelector
from middleware.message import Message
from middleware.listeners import HttpListener

from middleware.abstract.config_base import get_or_default
from middleware.message import is_response_success, has_no_cache_tag

#
# Setup logger
#

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

logger.setLevel(os.environ.setdefault("LOG_LEVEL", "INFO"))


class Cache(RoundRobinSelector):
    def __init__(self, alias: str, config: dict):
        config = {
            "cache_duration": get_or_default(config, "cache_duration", 2),
            "medium_duration_filter": get_or_default(
                config, "medium_duration_filter", []
            ),
            "medium_cache_duration": get_or_default(
                config, "medium_cache_duration", 30
            ),
            "long_duration_filter": get_or_default(config, "long_duration_filter", []),
            "long_cache_duration": get_or_default(config, "long_cache_duration", 86400),
        }
        super().__init__(alias, config)

        self.__statistics_listener = HttpListener(alias, config, self.__get_statistics)

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
        return f"Cache({self.alias}, {self.config})"

    def __str__(self) -> str:
        return f"Cache({self.alias})"

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

    def _on_application_setup(self, app: web.Application):
        self.__statistics_listener.do_setup_application(app)
        app.cleanup_ctx.append(self.__ctx_cleanup)
        app.add_routes(
            [
                web.post(
                    f"/{self.alias}/statistics",
                    self.__statistics_listener.handle_request,
                )
            ]
        )

    async def __get_statistics(self, _) -> Message:
        """Resolve '/cache/statistics' request"""
        return Message(self.__statistics_dict)

    async def __ctx_cleanup(self, _):
        tasks = [
            asyncio.create_task(
                self.__cache_cleaner(
                    self.__general_purpose_cache, self.config["cache_duration"]
                )
            ),
            asyncio.create_task(
                self.__cache_cleaner(
                    self.__medium_duration_cache, self.config["medium_cache_duration"]
                )
            ),
            asyncio.create_task(
                self.__cache_cleaner(
                    self.__long_duration_cache, self.config["long_cache_duration"]
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
        if not has_no_cache_tag(request) and is_response_success(response):
            method = request.as_json("method")
            if method in self.config["medium_duration_filter"]:
                self.__medium_duration_cache[request] = response
            elif method in self.config["long_duration_filter"]:
                self.__long_duration_cache[request] = response
            else:
                self.__general_purpose_cache[request] = response

    def __retrive_response_from_cache(self, request: Message) -> Message:
        if not has_no_cache_tag(request):
            if request in self.__medium_duration_cache:
                return self.__medium_duration_cache[request]
            if request in self.__long_duration_cache:
                return self.__long_duration_cache[request]
            if request in self.__general_purpose_cache:
                return self.__general_purpose_cache[request]
