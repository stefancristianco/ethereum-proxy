"""
Load balancer.
"""

import asyncio
import logging
import os

from aiohttp import web
from contextlib import suppress

from extensions.abstract.extension_base import Extension, ExtensionBase
from utils.message import Message

from utils.helpers import log_exception, get_or_default


#
# Setup logger
#

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

logger.setLevel(os.environ.setdefault("LOG_LEVEL", "INFO"))

#
# List of strings to indentify unsupported API calls
#

API_NOT_SUPPORTED_FILTERS = {
    # Blast
    "Method not found",
    "Capacity exceeded",
    # Others
    "does not exist/is not available",
}


class LoadBalancer(Extension):
    def __init__(self, alias: str, config: dict):
        super().__init__(alias, config)

        self.__blacklist_duration = get_or_default(config, "blacklist_duration", 3600)

        self.__next_handlers = []
        self.__method_table = {}
        self.__blacklist = {}

    def __repr__(self) -> str:
        return f"LoadBalancer({self.get_alias()}, {self.get_config()})"

    def __str__(self) -> str:
        return f"LoadBalancer({self.get_alias()})"

    async def _handle_request(self, request: Message) -> Message:
        request_obj = request.as_json()
        method = request_obj["method"]
        if not method in self.__method_table:
            self.__method_table[method] = {
                "hit_count": 0,
                "endpoints": {f"{handler}" for handler in self.__next_handlers},
            }
        self.__method_table[method]["hit_count"] += 1

        first = self.__method_table[method]["hit_count"]
        last = first + len(self.__next_handlers)
        for index in range(first, last):
            next_handler = self.__next_handlers[index % len(self.__next_handlers)]
            if not f"{next_handler}" in self.__method_table[method]["endpoints"]:
                # This handler was blacklisted for current method
                continue
            try:
                return self.__check_api_support(
                    await next_handler.do_handle_request(request), method
                )
            except Exception as ex:
                log_exception(logger, ex)
                # Blacklist endpoint on exception
                self.__method_table[method]["endpoints"].discard(f"{next_handler}")
                if not method in self.__blacklist:
                    self.__blacklist[method] = []
                self.__blacklist[method].append({f"{next_handler}": f"{ex}"})
        raise Exception(f"{method} not supported")

    def _get_routes(self, prefix: str) -> list:
        return [
            web.post(f"{prefix}/statistics", self.__get_statistics),
            web.post(f"{prefix}/blacklist", self.__get_blacklist),
        ]

    async def __get_statistics(self, _: web.Request) -> web.Response:
        """Resolve '/.../statistics' request"""
        output = {}
        for method in self.__method_table:
            output[method] = {
                "hit_count": self.__method_table[method]["hit_count"],
                "endpoints": list(self.__method_table[method]["endpoints"]),
            }
        return web.json_response(output)

    async def __get_blacklist(self, _: web.Request) -> web.Response:
        """Resolve '/.../blacklist' request"""
        return web.json_response(self.__blacklist)

    def _add_next_handler(self, next_handler: ExtensionBase):
        self.__next_handlers.append(next_handler)

    async def ctx_cleanup(self, _):
        blacklist_task = asyncio.create_task(self.__manage_blacklisted_endpoints())

        yield

        blacklist_task.cancel()
        with suppress(asyncio.CancelledError):
            await blacklist_task

    def __check_api_support(self, response: Message, method_name: str) -> Message:
        if len(response) > 512:
            # Optimization: error messages are small in size
            return response
        response_obj = response.as_json()
        if not "error" in response_obj or not "message" in response_obj["error"]:
            return response
        error_message = response_obj["error"]["message"]
        for msg in API_NOT_SUPPORTED_FILTERS:
            if error_message.find(msg) > -1:
                raise Exception(f"{method_name} not supported: {response}")
        return response

    async def __manage_blacklisted_endpoints(self):
        while True:
            await asyncio.sleep(self.__blacklist_duration)
            for method in self.__method_table:
                self.__method_table[method]["endpoints"] = {
                    f"{handler}" for handler in self.__next_handlers
                }
            self.__blacklist.clear()
