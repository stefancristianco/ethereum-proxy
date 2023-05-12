"""
Publish/Subscribe component
"""

import logging
import os

from components.abstract.round_robin_selector import RoundRobinSelector
from middleware.message import Message

from middleware.message import make_message_with_result


#
# Setup logger
#

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

logger.setLevel(os.environ.setdefault("LOG_LEVEL", "INFO"))


class PubSub(RoundRobinSelector):
    def __init__(self, alias: str, config: dict):
        super().__init__(alias, config)

        self.__pubsub_table = {
            "eth_subscribe": self.__handle_eth_subscribe,
            "eth_unsubscribe": self.__handle_eth_unsubscribe,
        }
        self.__subscription_number = int("0x10000000000000000000000000000000", 0)

    def __repr__(self) -> str:
        return f"PubSub({self.alias}, {self.config})"

    def __str__(self) -> str:
        return f"PubSub({self.alias})"

    async def _handle_request(self, request: Message) -> Message:
        if request.as_json("method") in self.__pubsub_table:
            return await self.__pubsub_table[request.as_json("method")](request)
        return await super()._handle_request(request)

    async def __handle_eth_subscribe(self, request: Message) -> Message:
        subscription = self.__subscription_number
        self.__subscription_number += 1
        return make_message_with_result(hex(subscription))

    async def __handle_eth_unsubscribe(self, request: Message) -> Message:
        return make_message_with_result(True)
