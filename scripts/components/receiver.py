"""
RPC entrypoint provider for ethereum compatible chains.
"""

import logging
import os

from aiohttp import web

from middleware.message import Message
from middleware.listeners import WsListener, HttpListener
from components.abstract.round_robin_selector import RoundRobinSelector

from middleware.helpers import get_or_default

#
# Setup logger
#

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

logger.setLevel(os.environ.setdefault("LOG_LEVEL", "INFO"))


class Receiver(RoundRobinSelector):
    def __init__(self, alias: str, config: dict):
        # Fill config with missing default values
        config["max_request_size"] = get_or_default(
            config, "max_request_size", 10 * 1024  # 10KB
        )

        super().__init__(alias, config)

        self.__ws_listener = WsListener(alias, config, self.do_handle_request)
        self.__http_listener = HttpListener(alias, config, self.do_handle_request)

    def __repr__(self) -> str:
        return f"Receiver({self.get_alias()}, {self.get_config()})"

    def __str__(self) -> str:
        return f"Receiver({self.get_alias()})"

    def _on_application_setup(self, app: web.Application):
        self.__ws_listener.do_setup_application(app)
        self.__http_listener.do_setup_application(app)
        app.add_routes(
            [
                web.post(f"/{self.get_alias()}/", self.__http_listener.handle_request),
                web.get(f"/{self.get_alias()}/ws", self.__ws_listener.handle_request),
            ]
        )

    async def _handle_request(self, message: Message) -> Message:
        return await super()._handle_request(message)
