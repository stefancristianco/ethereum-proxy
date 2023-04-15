"""
Dump request/response
"""

import logging
import os

from extensions.abstract.round_robin_selector import RoundRobinSelector
from utils.message import Message


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
        super().__init__(alias, config)

    def __repr__(self) -> str:
        return f"Debug({self.get_alias()}, {self.get_config()})"

    def __str__(self) -> str:
        return f"Debug({self.get_alias()})"

    async def _handle_request(self, request: Message) -> Message:
        logger.debug(f"{request=}")
        response = await super()._handle_request(request)
        logger.debug(f"{response=}"[:256])
        return response
