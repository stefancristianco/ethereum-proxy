"""
Replace block tag (e.g. latest, earliest) with block number.
"""

import asyncio
import logging
import os

from contextlib import suppress

from extensions.abstract.round_robin_selector import RoundRobinSelector
from utils.message import Message, EthNotSupported, EthInvalidParams

from utils.message import make_request_message, make_message_with_result
from utils.helpers import log_exception, get_or_default


#
# Setup logger
#

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

logger.setLevel(os.environ.setdefault("LOG_LEVEL", "INFO"))


class Optimizer(RoundRobinSelector):
    def __init__(self, alias: str, config: dict):
        super().__init__(alias, config)

        self.__pooling_interval = get_or_default(config, "pooling_interval", 2)
        self.__retries_count = get_or_default(config, "retries_count", 10)

        self.__block_number = 0
        self.__optimizations_table = {
            # Ethereum API
            "eth_accounts": self.__optimize_eth_accounts,
            "eth_chainId": self.__optimize_eth_chain_id,
            "eth_blockNumber": self.__optimize_eth_block_number,
            "eth_getBlockByNumber": self.__optimize_eth_get_block_by_number,
            "eth_getLogs": self.__optimize_eth_get_logs,
            # Trace API
            "trace_block": self.__optimize_trace_block,
        }

    def __repr__(self) -> str:
        return f"Optimizer({self.get_alias()}, {self.get_config()})"

    def __str__(self) -> str:
        return f"Optimizer({self.get_alias()})"

    async def _handle_request(self, request: Message) -> Message:
        if not self.__block_number:
            raise Exception("Optimizer not ready")
        request_obj = request.as_json()
        if request_obj["method"] in self.__optimizations_table:
            return await self.__optimizations_table[request_obj["method"]](request)
        return await super()._handle_request(request)

    async def ctx_cleanup(self, _):
        block_number_fetcher = asyncio.create_task(self.__fetch_block_number())

        yield

        block_number_fetcher.cancel()
        with suppress(asyncio.CancelledError):
            await block_number_fetcher

    async def __fetch_block_number_with_retries(self) -> int:
        for _ in range(self.__retries_count):
            try:
                response = await super()._handle_request(
                    make_request_message("eth_blockNumber")
                )
                response_obj = response.as_json()
                new_block_number = int(response_obj["result"], 0)
            except asyncio.CancelledError:
                # Allow this exception to break the loop during shutdown
                raise
            except Exception as ex:
                log_exception(logger, ex)
            else:
                if new_block_number >= self.__block_number:
                    return new_block_number
        return self.__block_number

    async def __fetch_block_number(self):
        while True:
            self.__block_number = await self.__fetch_block_number_with_retries()
            await asyncio.sleep(self.__pooling_interval)

    async def __optimize_eth_accounts(self, _: Message) -> Message:
        return make_message_with_result()

    async def __optimize_eth_block_number(self, _: Message) -> Message:
        return make_message_with_result(hex(self.__block_number))

    async def __optimize_eth_chain_id(self, request: Message) -> Message:
        return await self.__optimize_request_without_params(request)

    async def __optimize_eth_get_block_by_number(self, request: Message) -> Message:
        return await self.__optimize_request_with_tag_in_params(request, 0)

    async def __optimize_eth_get_logs(self, request: Message) -> Message:
        return await self.__optimize_request_with_from_to_block(request)

    async def __optimize_trace_block(self, request: Message) -> Message:
        return await self.__optimize_request_with_tag_in_params(request, 0)

    async def __optimize_request_without_params(self, request: Message) -> Message:
        request_obj = request.as_json()
        return await super()._handle_request(
            make_request_message(request_obj["method"])
        )

    async def __optimize_request_with_tag_in_params(
        self, request: Message, pos: int
    ) -> Message:
        request_obj = request.as_json_copy()
        params = request_obj["params"]

        params[pos] = self.__check_and_replace_tag(params[pos], request_obj["method"])

        if int(params[pos], 0) > self.__block_number:
            # Optimization: return null result for block in the future
            return make_message_with_result()
        return await super()._handle_request(
            make_request_message(request_obj["method"], params)
        )

    async def __optimize_request_with_from_to_block(self, request: Message) -> Message:
        request_obj = request.as_json_copy()
        params = request_obj["params"]

        # use default:latest for block ranges
        obj = params[0]
        if not "blockHash" in obj:
            if not "fromBlock" in obj:
                obj["fromBlock"] = hex(self.__block_number)
            if not "toBlock" in obj:
                obj["toBlock"] = hex(self.__block_number)
            obj["fromBlock"] = self.__check_and_replace_tag(
                obj["fromBlock"], request_obj["method"]
            )
            obj["toBlock"] = self.__check_and_replace_tag(
                obj["toBlock"], request_obj["method"]
            )
            fromBlock = int(obj["fromBlock"], 0)
            toBlock = int(obj["toBlock"], 0)
            if fromBlock != toBlock:
                if fromBlock > toBlock:
                    raise EthInvalidParams(
                        f"{request_obj['method']}: invalid block range"
                    )
                raise EthNotSupported(
                    f"{request_obj['method']}: range too big, only single entry accepted"
                )
            if fromBlock > self.__block_number:
                # Optimization: return null result for block in the future
                return make_message_with_result()
        return await super()._handle_request(
            make_request_message(request_obj["method"], params)
        )

    def __check_and_replace_tag(self, tag: str, desc: str) -> str:
        if tag in ["earliest", "pending"]:
            raise EthNotSupported(f"{desc}: tag '{tag}' not supported")
        if tag == "latest":
            return hex(self.__block_number)
        return hex(int(tag, 0))
