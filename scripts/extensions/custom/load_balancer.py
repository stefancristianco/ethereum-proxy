"""
Load balancer.
"""

import asyncio
import logging
import os

from aiohttp import web
from typing import List
from contextlib import suppress

from extensions.extension_base import Extension, ExtensionBase
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

# Blacklist duration in seconds
blacklist_duration = int(os.environ.setdefault("PROXY_LB_BLACKLIST_DURATION", "3600"))

logger.info("========= Lb Globals =========")
logger.info(f"PROXY_LB_BLACKLIST_DURATION: {blacklist_duration}")

#
# List of strings to indentify unsupported API calls
#

API_NOT_SUPPORTED_FILTERS = {
    # Blast
    "not found",
    # Alchemy
    "is not available",
    # Others
    "does not exist",
}


class LoadBalancer(Extension):
    def __init__(self):
        super().__init__("LoadBalancer")

        self.__next_handlers = []
        self.__method_table = {}
        self.__blacklist = {}

    def __repr__(self) -> str:
        return "LoadBalancer()"

    async def _handle_request(self, request: Message) -> Message:
        request_obj = request.as_json()
        method = request_obj["method"]
        if not method in self.__method_table:
            self.__method_table[method] = {
                "hit_count": 0,
                "endpoints": {handler.get_name() for handler in self.__next_handlers},
            }
        self.__method_table[method]["hit_count"] += 1

        first = self.__method_table[method]["hit_count"]
        last = first + len(self.__next_handlers)
        for index in range(first, last):
            next_handler = self.__next_handlers[index % len(self.__next_handlers)]
            if not next_handler.get_name() in self.__method_table[method]["endpoints"]:
                # This handler was blacklisted for current method
                continue
            try:
                return self.__check_api_support(
                    await next_handler.do_handle_request(request)
                )
            except Exception as ex:
                # Blacklist endpoint on exception
                self.__method_table[method]["endpoints"].discard(
                    next_handler.get_name()
                )
                if not method in self.__blacklist:
                    self.__blacklist[method] = []
                self.__blacklist[method].append({str(next_handler): str(ex)})
        raise Exception(f"{method} not supported")

    def _get_routes(self) -> List:
        return [
            ("/lb/statistics", self.__get_statistics),
            ("/lb/blacklist", self.__get_blacklist),
        ]

    async def __get_statistics(self, request: web.Request) -> web.Response:
        """Resolve '/lb/statistics' request"""
        output = {}
        for method in self.__method_table:
            output[method] = {
                "hit_count": self.__method_table[method]["hit_count"],
                "endpoints": list(self.__method_table[method]["endpoints"]),
            }
        return web.json_response(output)

    async def __get_blacklist(self, request: web.Request) -> web.Response:
        """Resolve '/lb/blacklist' request"""
        return web.json_response(self.__blacklist)

    def _add_next_handler(self, next_handler: ExtensionBase) -> None:
        self.__next_handlers.append(next_handler)

    async def _initialize(self) -> None:
        self.__blacklist_task = asyncio.create_task(
            self.__manage_blacklisted_endpoints()
        )
        for handler in self.__next_handlers:
            await handler.do_initialize()

    async def _cancel(self) -> None:
        self.__blacklist_task.cancel()
        with suppress(asyncio.CancelledError):
            await self.__blacklist_task
        for handler in self.__next_handlers:
            await handler.do_cancel()

    def __check_api_support(self, msg: Message) -> Message:
        if len(msg) > 512:
            # Optimization: error messages are small in size
            return msg
        msg_obj = msg.as_json()
        if not "error" in msg_obj or not "message" in msg_obj["error"]:
            return msg
        error_message = msg_obj["error"]["message"]
        for str in API_NOT_SUPPORTED_FILTERS:
            if error_message.find(str) >= -1:
                raise Exception(f"Api not supported, from {msg}")
        return msg

    async def __manage_blacklisted_endpoints(self) -> None:
        while True:
            await asyncio.sleep(blacklist_duration)

            logger.info("Reinserting blacklisted endpoints")
            for method in self.__method_table:
                self.__method_table[method]["endpoints"] = {
                    handler.get_name() for handler in self.__next_handlers
                }
            self.__blacklist.clear()
