"""
Load balancer.
"""

import asyncio
import logging
import os

from aiohttp import web
from contextlib import suppress

from components.abstract.component import ComponentLink, Component
from middleware.message import Message, EthMethodNotSupported
from middleware.listeners import HttpListener

from middleware.helpers import log_exception, get_or_default


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


class LoadBalancer(ComponentLink):
    def __init__(self, alias: str, config: dict):
        config = {
            "blacklist_filters": get_or_default(config, "blacklist_filters", []),
            "blacklist_duration": get_or_default(config, "blacklist_duration", 3600),
        }
        super().__init__(alias, config)

        self.__statistics_listener = HttpListener(alias, config, self.__get_statistics)
        self.__blacklist_listener = HttpListener(alias, config, self.__get_blacklist)

        self.__next_handlers = []
        self.__method_table = {}
        self.__blacklist = {}

    def __repr__(self) -> str:
        return f"LoadBalancer({self.alias}, {self.config})"

    def __str__(self) -> str:
        return f"LoadBalancer({self.alias})"

    async def _handle_request(self, request: Message) -> Message:
        method = request.as_json("method")
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
                log_exception(logger, ex, f"lb-handler-request - {request}")
                # Blacklist endpoint on exception
                self.__method_table[method]["endpoints"].discard(f"{next_handler}")
                if not method in self.__blacklist:
                    self.__blacklist[method] = []
                self.__blacklist[method].append({f"{next_handler}": f"{ex}"})
        raise EthMethodNotSupported(f"{method} not supported")

    def _on_application_setup(self, app: web.Application):
        self.__statistics_listener.do_setup_application(app)
        self.__blacklist_listener.do_setup_application(app)
        app.cleanup_ctx.append(self.__ctx_cleanup)
        app.add_routes(
            [
                web.post(
                    f"/{self.alias}/statistics",
                    self.__statistics_listener.handle_request,
                ),
                web.post(
                    f"/{self.alias}/blacklist",
                    self.__blacklist_listener.handle_request,
                ),
            ]
        )

    async def __get_statistics(self, _) -> Message:
        """Resolve '/.../statistics' request"""
        output = {}
        for method in self.__method_table:
            output[method] = {
                "hit_count": self.__method_table[method]["hit_count"],
                "endpoints": list(self.__method_table[method]["endpoints"]),
            }
        return Message(output)

    async def __get_blacklist(self, _) -> Message:
        """Resolve '/.../blacklist' request"""
        return Message(self.__blacklist)

    def _add_next_handler(self, next_handler: Component):
        self.__next_handlers.append(next_handler)

    async def __ctx_cleanup(self, _):
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
        for msg in self.config["blacklist_filters"]:
            if error_message.find(msg) > -1:
                raise Exception(f"{method_name} not supported: {response}")
        return response

    async def __manage_blacklisted_endpoints(self):
        while True:
            await asyncio.sleep(self.config["blacklist_duration"])
            for method in self.__method_table:
                self.__method_table[method]["endpoints"] = {
                    f"{handler}" for handler in self.__next_handlers
                }
            self.__blacklist.clear()
