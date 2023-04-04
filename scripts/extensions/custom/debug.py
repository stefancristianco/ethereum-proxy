"""
Dump request/response
"""

import logging
import os

from extensions.round_robin_selector import RoundRobinSelector
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
    def __init__(self):
        super().__init__("Debug")

    def __repr__(self) -> str:
        return "Debug()"

    async def _handle_request(self, request: Message) -> Message:
        logger.debug(f"Request: {request}")
        response = await super()._handle_request(request)
        logger.debug(f"Response: {response}"[:256])
        return response
