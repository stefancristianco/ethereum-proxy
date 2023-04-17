"""
Replace block tag (e.g. latest, earliest) with block number.
"""

import asyncio
import logging
import os

from contextlib import suppress
from aiohttp import web

from components.abstract.round_robin_selector import RoundRobinSelector
from middleware.message import (
    Message,
    EthNotSupported,
    EthInvalidParams,
    EthLimitExceeded,
)

from middleware.message import (
    make_request_message,
    make_message_with_result,
    is_response_success,
)
from middleware.helpers import log_and_suppress, get_or_default


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
        self.__retries_count = get_or_default(config, "retries_count", 5)
        self.__max_prefetch_range = get_or_default(config, "max_prefetch_range", 20)
        self.__prefetch_list = get_or_default(
            config, "prefetch", ["eth_getLogs", "eth_getBlockByNumber"]
        )

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
        self.__prefetch_table = {
            # Ethereum API
            "eth_getBlockByNumber": self.__prefetch_eth_get_block_by_number,
            "eth_getLogs": self.__prefetch_eth_get_logs,
            # Trace API
            "trace_block": self.__prefetch_trace_block,
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

    def _on_application_setup(self, app: web.Application):
        app.cleanup_ctx.append(self.__ctx_cleanup)

    async def __ctx_cleanup(self, _):
        prefetcher = asyncio.create_task(self.__prefetch_data())

        yield

        prefetcher.cancel()
        with suppress(asyncio.CancelledError):
            await prefetcher

    async def __fetch_block_number_with_retries(self) -> int:
        msg = make_request_message("eth_blockNumber")
        for _ in range(self.__retries_count):
            with log_and_suppress(logger, f"prefetch-block-number - {msg}"):
                response = await super()._handle_request(msg)
                new_block_number = int(response.as_json("result"), 0)
                if new_block_number >= self.__block_number:
                    return new_block_number
        return self.__block_number

    async def __prefetch_with_retries(self, msg: Message):
        for _ in range(self.__retries_count):
            with log_and_suppress(logger, f"prefetch-data - {msg}"):
                if is_response_success(await super()._handle_request(msg)):
                    return

    async def __prefetch_eth_get_block_by_number(self, block_number: int):
        await self.__prefetch_with_retries(
            make_request_message("eth_getBlockByNumber", [hex(block_number), True])
        )

    async def __prefetch_eth_get_logs(self, block_number: int):
        await self.__prefetch_with_retries(
            make_request_message(
                "eth_getLogs",
                [
                    {
                        "fromBlock": hex(block_number),
                        "toBlock": hex(block_number),
                    }
                ],
            )
        )

    async def __prefetch_trace_block(self, block_number: int):
        await self.__prefetch_with_retries(
            make_request_message("trace_block", [hex(block_number)])
        )

    async def __prefetch_data(self):
        while True:
            first = self.__block_number + 1
            self.__block_number = await self.__fetch_block_number_with_retries()
            last = self.__block_number + 1
            if last - first > self.__max_prefetch_range:
                # Fast forward to present block
                first = last - 1

            tasks = [asyncio.sleep(self.__pooling_interval)]
            # Trigger prefetch in the range [first, last)
            for block_number in range(first, last):
                for method in self.__prefetch_list:
                    assert method in self.__prefetch_table
                    tasks.append(self.__prefetch_table[method](block_number))
            await asyncio.gather(*tasks, return_exceptions=True)

    async def __optimize_eth_accounts(self, _: Message) -> Message:
        return make_message_with_result()

    async def __optimize_eth_block_number(self, _: Message) -> Message:
        return make_message_with_result(hex(self.__block_number))

    async def __optimize_eth_chain_id(self, request: Message) -> Message:
        return await self.__optimize_request_without_params(request)

    async def __optimize_eth_get_block_by_number(self, request: Message) -> Message:
        return await self.__optimize_request_with_tag_in_params(request, 0)

    async def __optimize_eth_get_logs(self, request: Message) -> Message:
        request_obj = request.as_json()
        # Normalize request for better cache use
        normalized_args = {}
        original_args = request_obj["params"][0]
        if "blockHash" in original_args:
            normalized_args["blockHash"] = original_args["blockHash"]
        else:
            if "fromBlock" in original_args:
                from_block = self.__check_and_replace_tag(
                    original_args["fromBlock"], request_obj["method"]
                )
            else:
                from_block = hex(self.__block_number)

            if "toBlock" in original_args:
                to_block = self.__check_and_replace_tag(
                    original_args["toBlock"], request_obj["method"]
                )
            else:
                to_block = hex(self.__block_number)
            normalized_args["fromBlock"] = from_block
            normalized_args["toBlock"] = to_block

            _from = int(from_block, 0)
            _to = int(to_block, 0)
            if _from != _to:
                if _from > _to:
                    raise EthInvalidParams(
                        f"{request_obj['method']}: invalid block range"
                    )
                raise EthLimitExceeded(
                    f"{request_obj['method']}: range too big, only single entry accepted"
                )
            if _from > self.__block_number:
                # Optimization: return null result for block in the future
                return make_message_with_result()
        if "topics" in original_args:
            normalized_args["topics"] = original_args["topics"]
        return await super()._handle_request(
            make_request_message(request_obj["method"], [normalized_args])
        )

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

    def __check_and_replace_tag(self, tag: str, desc: str) -> str:
        if tag in ["earliest", "pending"]:
            raise EthNotSupported(f"{desc}: tag '{tag}' not supported")
        if tag == "latest":
            return hex(self.__block_number)
        return hex(int(tag, 0))
