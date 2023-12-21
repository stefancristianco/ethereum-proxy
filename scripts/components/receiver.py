"""
RPC entrypoint provider for ethereum compatible chains.
"""

import asyncio
import logging
import os

from middleware.message import Message
from components.abstract.round_robin_selector import RoundRobinSelector

from middleware.helpers import log_on_exception_decorator, concat_bytes, entrypoint
from middleware.message import make_response_from_exception

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
        super().__init__(alias, config)

    def __repr__(self) -> str:
        return f"Receiver({self.alias}, {self.config})"

    def __str__(self) -> str:
        return f"Receiver({self.alias})"

    @entrypoint
    async def handle_request(self, request: Message) -> Message:
        try:
            return await self.do_handle_request(request)
        except Exception as ex:
            return make_response_from_exception(ex)

    @log_on_exception_decorator(logger)
    async def _handle_request(self, request: Message) -> Message:
        request_obj = request.as_json()
        is_batch_request = isinstance(request_obj, list)

        tasks = []
        if is_batch_request:
            # Handle batch request one by one
            for single_req in request_obj:
                tasks.append(super()._handle_request(Message(single_req)))
        else:
            tasks.append(super()._handle_request(request))

        result = await asyncio.gather(*tasks, return_exceptions=True)

        response_parts = []
        for single_response in result:
            if response_parts:
                response_parts.append(b",")
            if issubclass(type(single_response), Message):
                response_parts.append(single_response.as_raw_data())
            else:
                response_parts.append(
                    make_response_from_exception(single_response).as_raw_data()
                )

        if is_batch_request:
            response_parts = [b"[", *response_parts, b"]"]
        return Message(concat_bytes(response_parts))
