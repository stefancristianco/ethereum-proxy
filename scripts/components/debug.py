"""
Dump request/response
"""

import logging
import os

from components.abstract.round_robin_selector import RoundRobinSelector
from middleware.message import Message

from middleware.helpers import get_or_default


#
# Setup logger
#

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

logger.setLevel(os.environ.setdefault("LOG_LEVEL", "INFO"))


class Debug(RoundRobinSelector):
    def __init__(self, alias: str, config: dict):
        config["select_methods"] = get_or_default(config, "select_methods", [])
        super().__init__(alias, config)

    def __repr__(self) -> str:
        return f"Debug({self.alias}, {self.config})"

    def __str__(self) -> str:
        return f"Debug({self.alias})"

    async def _handle_request(self, request: Message) -> Message:
        if (
            not self.config["select_methods"]
            or request.as_json("method") in self.config["select_methods"]
        ):
            logger.debug(f"{request=}")
        response = await super()._handle_request(request)
        if (
            not self.config["select_methods"]
            or request.as_json("method") in self.config["select_methods"]
        ):
            logger.debug(f"{response=}"[:256])
        return response
