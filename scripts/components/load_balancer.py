"""
Load balancer.
"""

import asyncio
import logging
import os

from aiohttp import web
from contextlib import suppress

from components.abstract.component import ComponentLink, Component
from middleware.message import (
    Message,
    EthMethodNotSupported,
    EthMethodNotFound,
    EthCapacityExceeded,
)

from middleware.helpers import log_and_suppress, get_or_default, entrypoint


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
        config["blacklist_duration"] = get_or_default(
            config, "blacklist_duration", 3600
        )
        super().__init__(alias, config)

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
            with log_and_suppress(
                logger, f"lb-handle-request - {next_handler} - {request}"
            ):
                if not f"{next_handler}" in self.__method_table[method]["endpoints"]:
                    # This handler was blacklisted for current method
                    continue
                try:
                    return await next_handler.do_handle_request(request)
                except (
                    EthMethodNotFound,
                    EthMethodNotSupported,
                    EthCapacityExceeded,
                ) as ex:
                    logger.warning(f"Blacklisting {next_handler} - {ex}")
                    self.__method_table[method]["endpoints"].discard(f"{next_handler}")
                    if not method in self.__blacklist:
                        self.__blacklist[method] = []
                    self.__blacklist[method].append({f"{next_handler}": f"{ex}"})
        raise EthMethodNotSupported(f"{method} not supported")

    def _on_application_setup(self, app: web.Application):
        app.cleanup_ctx.append(self.__ctx_cleanup)

    @entrypoint
    async def statistics(self, _) -> Message:
        """Resolve '/.../statistics' request"""
        output = {}
        for method in self.__method_table:
            output[method] = {
                "hit_count": self.__method_table[method]["hit_count"],
                "endpoints": list(self.__method_table[method]["endpoints"]),
            }
        return Message(output)

    @entrypoint
    async def blacklist(self, _) -> Message:
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

    async def __manage_blacklisted_endpoints(self):
        while True:
            await asyncio.sleep(self.config["blacklist_duration"])
            for method in self.__method_table:
                self.__method_table[method]["endpoints"] = {
                    f"{handler}" for handler in self.__next_handlers
                }
            self.__blacklist.clear()
