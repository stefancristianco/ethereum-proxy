"""
Replace block tag (e.g. latest, earliest) with block number.
"""

import asyncio
import logging
import os
import json

from contextlib import suppress

from extensions.round_robin_selector import RoundRobinSelector
from utils.message import Message, EthNotSupported, EthInvalidParams

from utils.message import make_request_message, make_message_with_result
from utils.helpers import log_exception


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

# How often to query for the latest block number (chain dependent)
pooling_interval = int(os.environ.setdefault("PROXY_TAG_POOLING_INTERVAL", str(10)))

logger.info("======= TagReplacer Globals =======")
logger.info(f"PROXY_TAG_POOLING_INTERVAL: {pooling_interval} seconds")


class TagReplacer(RoundRobinSelector):
    def __init__(self):
        super().__init__("TagReplacer")
        self.__block_number = 0

        self.__tag_replace_table = {
            # Ethereum API
            "eth_blockNumber": self.__handle_eth_block_number,
            "eth_getBalance": self.__handle_request_with_tag_in_params,
            "eth_getBlockByNumber": self.__handle_request_with_tag_in_params,
            "eth_getBlockTransactionCountByNumber": self.__handle_request_with_tag_in_params,
            "eth_getCode": self.__handle_request_with_tag_in_params,
            "eth_getLogs": self.__handle_request_with_from_to_block,
            # Trace API
            "trace_block": self.__handle_request_with_tag_in_params,
        }
        self.__optimizations_table = {
            # Ethereum API
            "eth_getBlockByNumber": self.__optimize_request_with_tag_in_params_pos0,
            "eth_getLogs": self.__optimize_request_with_from_to_block,
            # Trace API
            "trace_block": self.__optimize_request_with_tag_in_params_pos0,
        }

    def __repr__(self) -> str:
        return "TagReplacer()"

    async def _handle_request(self, request: Message) -> Message:
        if not self.__block_number:
            raise Exception("Extension tag_replacer not ready")
        request_obj = request.as_json()
        if request_obj["method"] in self.__tag_replace_table:
            return await self.__tag_replace_table[request_obj["method"]](request)
        return await super()._handle_request(request)

    async def _initialize(self):
        self.__block_number_fetcher = asyncio.create_task(self.__fetch_block_number())
        await super()._initialize()

    async def _cancel(self):
        self.__block_number_fetcher.cancel()
        with suppress(asyncio.CancelledError):
            await self.__block_number_fetcher
        await super()._cancel()

    async def __fetch_block_number(self):
        while True:
            try:
                response = await super()._handle_request(
                    make_request_message("eth_blockNumber")
                )
                response_obj = response.as_json()
                new_block_number = int(response_obj["result"], 0)
                if new_block_number > self.__block_number:
                    self.__block_number = new_block_number
            except asyncio.CancelledError:
                # Allow this exception to break the loop during shutdown
                raise
            except Exception as ex:
                log_exception(logger, ex)
            await asyncio.sleep(pooling_interval)

    async def __handle_eth_block_number(self, _: Message) -> Message:
        return make_message_with_result(hex(self.__block_number))

    async def __handle_request_with_tag_in_params(self, request: Message) -> Message:
        request_obj = request.as_json()
        params = request_obj["params"]
        for tag in ["earliest", "pending"]:
            if tag in params:
                raise EthNotSupported(
                    f"{request_obj['method']}: tag '{tag}' not supported"
                )
        if "latest" in params:
            tag_index = params.index("latest")
            params[tag_index] = hex(self.__block_number)
        if request_obj["method"] in self.__optimizations_table:
            return await self.__optimizations_table[request_obj["method"]](request_obj)
        return await super()._handle_request(Message(json.dumps(request_obj)))

    async def __handle_request_with_from_to_block(self, request: Message) -> Message:
        request_obj = request.as_json()
        params_obj = request_obj["params"][0]
        # use default:latest for block ranges
        if not "blockHash" in params_obj:
            if not "fromBlock" in params_obj:
                params_obj["fromBlock"] = "latest"
            if not "toBlock" in params_obj:
                params_obj["toBlock"] = "latest"
            params_obj["fromBlock"] = self.__check_and_replace_tag(
                params_obj["fromBlock"], request_obj["method"]
            )
            params_obj["toBlock"] = self.__check_and_replace_tag(
                params_obj["toBlock"], request_obj["method"]
            )
            fromBlock = int(params_obj["fromBlock"], 0)
            toBlock = int(params_obj["toBlock"], 0)
            if fromBlock != toBlock:
                if fromBlock > toBlock:
                    raise EthInvalidParams(
                        f"{request_obj['method']}: invalid block range"
                    )
                raise EthNotSupported(
                    f"{request_obj['method']}: range too big, only single entry accepted"
                )
            if request_obj["method"] in self.__optimizations_table:
                return await self.__optimizations_table[request_obj["method"]](
                    request_obj
                )
        return await super()._handle_request(Message(json.dumps(request_obj)))

    async def __optimize_request_with_tag_in_params_pos0(self, request_obj) -> Message:
        return await self.__optimize_request_with_tag_in_params(request_obj, 0)

    async def __optimize_request_with_from_to_block(self, request_obj) -> Message:
        if int(request_obj["params"][0]["fromBlock"], 0) > self.__block_number:
            return make_message_with_result()
        return await super()._handle_request(Message(json.dumps(request_obj)))

    async def __optimize_request_with_tag_in_params(
        self, request_obj, pos: int
    ) -> Message:
        if int(request_obj["params"][pos], 0) > self.__block_number:
            return make_message_with_result()
        return await super()._handle_request(Message(json.dumps(request_obj)))

    def __check_and_replace_tag(self, tag: str, desc: str) -> str:
        if tag == "earliest" or tag == "pending":
            raise EthNotSupported(f"{desc}: tag '{tag}' not supported")
        if tag == "latest":
            return hex(self.__block_number)
        return tag
