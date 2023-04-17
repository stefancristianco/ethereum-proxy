"""
Endpoint connection.
"""

import logging
import os

from aiohttp import web

from middleware.message import Message
from middleware.endpoints import JsonRpcEndpoint
from components.abstract.component import ComponentLink

from middleware.helpers import get_or_default, unreachable_code

#
# Setup logger
#

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

logger.setLevel(os.environ.setdefault("LOG_LEVEL", "INFO"))


class Endpoint(ComponentLink):
    def __init__(self, alias: str, config: dict):
        # Fill config with missing default values
        config["tasks_count"] = get_or_default(config, "tasks_count", 10)
        config["max_queue_size"] = get_or_default(config, "max_queue_size", 100)
        config["max_response_size"] = get_or_default(
            config, "max_response_size", 100 * 2**20  # 100MB
        )
        config["response_timeout"] = get_or_default(config, "response_timeout", 10)
        config["check_ssl"] = get_or_default(config, "check_ssl", False)

        super().__init__(alias, config)

        self.__endpoint = JsonRpcEndpoint(alias, config)

    def __repr__(self) -> str:
        return f"Endpoint({self.get_alias()}, {self.get_config()})"

    def __str__(self) -> str:
        return f"Endpoint({self.get_alias()})"

    async def _handle_request(self, request: Message) -> Message:
        return await self.__endpoint.send_request(request)

    def _on_application_setup(self, app: web.Application):
        self.__endpoint.do_setup_application(app)

    def _add_next_handler(self, _):
        unreachable_code()