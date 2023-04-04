"""
Replace block tag (e.g. latest, earliest) with block number.
"""

import asyncio
import logging
import os
import json

from contextlib import suppress

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

#
# Environment variables and global constants
#

# How often to query for the latest block number (chain dependent)
pooling_interval = int(os.environ.setdefault("PROXY_TAG_POOLING_INTERVAL", str(5)))

logger.info("======= Latest Globals =======")
logger.info(f"PROXY_TAG_POOLING_INTERVAL: {pooling_interval} seconds")


class TagReplacer(RoundRobinSelector):
    def __init__(self):
        super().__init__("TagReplacer")
        self.__block_number = 0

        self.__tag_replace_table = {
            # Ethereum API
            "eth_blockNumber": self.__handle_block_number,
            "eth_getBlockByNumber": self.__handle_request_with_tag_in_params,
            "eth_getBlockTransactionCountByNumber": self.__handle_request_with_tag_in_params,
            "eth_getCode": self.__handle_request_with_tag_in_params,
            "eth_getLogs": self.__handle_request_with_from_to_block,
            "eth_getProof": self.__handle_request_with_tag_in_params,
            "eth_getStorageAt": self.__handle_request_with_tag_in_params,
            "eth_getTransactionByBlockNumberAndIndex": self.__handle_request_with_tag_in_params,
            "eth_getTransactionCount": self.__handle_request_with_tag_in_params,
            "eth_getUncleByBlockNumberAndIndex": self.__handle_request_with_tag_in_params,
            "eth_getUncleCountByBlockNumber": self.__handle_request_with_tag_in_params,
            "eth_getProof": self.__handle_request_with_tag_in_params,
            # Trace API
            "trace_block": self.__handle_request_with_tag_in_params,
            "trace_call": self.__handle_request_with_tag_in_params,
            "trace_callMany": self.__handle_request_with_tag_in_params,
            "trace_filter": self.__handle_request_with_from_to_block,
            "trace_replayBlockTransactions": self.__handle_request_with_tag_in_params,
        }
        self.__optimizations_table = {
            "eth_getBlockByNumber": self.__optimize_request_with_tag_in_params,
            "eth_getLogs": self.__optimize_request_with_from_to_block,
            "trace_filter": self.__optimize_request_with_from_to_block,
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
                    Message(
                        json.dumps(
                            {"jsonrpc": "2.0", "id": 0, "method": "eth_blockNumber"}
                        )
                    )
                )
                response_obj = response.as_json()
                new_block_number = int(response_obj["result"], 0)
                if new_block_number > self.__block_number:
                    self.__block_number = new_block_number
            except asyncio.CancelledError:
                # Allow this exception to break the loop during shutdown
                raise
            except Exception as ex:
                logger.error(str(ex))
                if logger.level <= logging.DEBUG:
                    logger.exception("Caught exception")
            await asyncio.sleep(pooling_interval)

    async def __handle_block_number(self, request: Message) -> Message:
        request_obj = request.as_json()
        if "params" in request_obj and len(request_obj["params"]) > 0:
            raise Exception(f"eth_blockNumber: expected empty params")
        return Message(
            json.dumps({"jsonrpc": "2.0", "id": 0, "result": hex(self.__block_number)})
        )

    async def __handle_request_with_tag_in_params(self, request: Message) -> Message:
        request_obj = request.as_json()
        if not "params" in request_obj:
            raise Exception(f"{request_obj['method']}: missing params")
        params = request_obj["params"]
        for tag in ["earliest", "pending"]:
            if tag in params:
                raise Exception(f"{request_obj['method']}: tag '{tag}' not supported")
        if "latest" in params:
            tag_index = params.index("latest")
            params[tag_index] = hex(self.__block_number)
        if request_obj["method"] in self.__optimizations_table:
            return await self.__optimizations_table[request_obj["method"]](request_obj)
        return await super()._handle_request(Message(json.dumps(request_obj)))

    async def __optimize_request_with_tag_in_params(self, request_obj) -> Message:
        if int(request_obj["params"][0], 0) > self.__block_number:
            return Message(json.dumps({"jsonrpc": "2.0", "id": 0, "result": None}))
        return await super()._handle_request(Message(json.dumps(request_obj)))

    async def __handle_request_with_from_to_block(self, request: Message) -> Message:
        request_obj = request.as_json()
        if not "params" in request_obj:
            raise Exception(f"{request_obj['method']}: missing params")
        if not len(request_obj["params"]):
            raise Exception(f"{request_obj['method']}: unexpected empty params")
        params_obj = request_obj["params"][0]
        # use default:latest for block ranges
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
        if params_obj["fromBlock"] != params_obj["toBlock"]:
            raise Exception(
                f"{request_obj['method']}: range too big, only single entry accepted"
            )
        if request_obj["method"] in self.__optimizations_table:
            return await self.__optimizations_table[request_obj["method"]](request_obj)
        return await super()._handle_request(Message(json.dumps(request_obj)))

    async def __optimize_request_with_from_to_block(self, request_obj) -> Message:
        if int(request_obj["params"]["fromBlock"], 0) > self.__block_number:
            return Message(json.dumps({"jsonrpc": "2.0", "id": 0, "result": None}))
        return await super()._handle_request(Message(json.dumps(request_obj)))

    def __check_and_replace_tag(self, tag: str, desc: str) -> str:
        if tag == "earliest" or tag == "pending":
            raise Exception(f"{desc}: tag '{tag}' not supported")
        if tag == "latest":
            tag = hex(self.__block_number)
        return tag
