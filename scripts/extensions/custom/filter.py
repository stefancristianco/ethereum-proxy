"""
Validate and adjust requests
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
pooling_interval = int(os.environ.setdefault("PROXY_FILTER_POOLING_INTERVAL", str(2)))

logger.info("======= Latest Globals =======")
logger.info(f"PROXY_FILTER_POOLING_INTERVAL: {pooling_interval} seconds")


class Filter(RoundRobinSelector):
    def __init__(self):
        super().__init__("Filter")
        self.__block_number = 0

        self.__filter_validator_table = {
            "eth_blockNumber": self.__check_and_handle_block_number,
            "eth_call": self.__forward_request_generic,
            "eth_chainId": self.__forward_request_generic,
            "eth_estimateGas": self.__forward_request_generic,
            "eth_gasPrice": self.__forward_request_generic,
            "eth_getBalance": self.__forward_request_generic,
            "eth_getBlockByHash": self.__forward_request_generic,
            "eth_getBlockByNumber": self.__check_and_handle_get_block_by_number,
            "eth_gasPrice": self.__forward_request_generic,
            "trace_block": self.__check_and_handle_trace_block,
        }

    def __repr__(self) -> str:
        return "Filter()"

    async def _handle_request(self, request: Message) -> Message:
        if not self.__block_number:
            raise Exception("Filter extension not ready")
        await self.__check_and_handle_request(request)

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

    async def __check_and_handle_request(self, request: Message) -> Message:
        request_obj = request.as_json()
        if request_obj["method"] in self.__filter_validator_table:
            return await self.__filter_validator_table[request_obj["method"]](request)
        raise Exception(f"{request_obj['method']} not allowed")

    async def __check_and_handle_trace_block(self, request: Message) -> Message:
        request_obj = request.as_json()
        if not "params" in request_obj:
            raise Exception("trace_block: missing params")
        params = request_obj["params"]
        if len(params) != 1:
            # trace_block has exactly one argument
            raise Exception(f"trace_block: expected one argument, found {len(params)}")
        block_number = params[0]
        if block_number == "earliest" or block_number == "pending":
            raise Exception(f"trace_block: tag '{block_number}' not supported")
        if block_number == "latest":
            block_number = hex(self.__block_number)
        self.__ensure_is_number("trace_block", block_number)
        request_obj["params"] = [block_number]
        return await super()._handle_request(Message(json.dumps(request_obj)))

    async def __check_and_handle_block_number(self, request: Message) -> Message:
        request_obj = request.as_json()
        if "params" in request_obj and len(request_obj["params"]) > 0:
            raise Exception(f"eth_blockNumber: expected zero params")
        return Message(
            json.dumps({"jsonrpc": "2.0", "id": 0, "result": hex(self.__block_number)})
        )

    async def __forward_request_generic(self, request: Message) -> Message:
        return await super()._handle_request(request)

    async def __check_and_handle_get_block_by_number(self, request: Message) -> Message:
        request_obj = request.as_json()
        if not "params" in request_obj:
            raise Exception("eth_getBlockByNumber: missing params")
        params = request_obj["params"]
        if len(params) != 2:
            # eth_getBlockByNumber has exactly two arguments
            raise Exception(
                f"eth_getBlockByNumber: expected two arguments, found {len(params)}"
            )
        block_number = params[0]
        full_transactions = params[1]
        if block_number == "earliest" or block_number == "pending":
            raise Exception(f"eth_getBlockByNumber: tag '{block_number}' not supported")
        if block_number == "latest":
            block_number = hex(self.__block_number)
        self.__ensure_is_number("eth_getBlockByNumber", block_number)
        request_obj["params"] = [block_number, full_transactions]
        return await super()._handle_request(Message(json.dumps(request_obj)))

    def __ensure_is_number(self, desc: str, block_number):
        try:
            int(block_number, 0)
        except:
            raise Exception(f"{desc}: invalid number")
