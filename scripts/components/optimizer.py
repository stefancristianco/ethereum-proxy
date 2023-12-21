"""
Replace block tag (e.g. latest, earliest) with block number.
"""

import asyncio
import logging
import os

from contextlib import suppress
from aiohttp import web
from asyncio import Event

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
    set_no_cache_tag,
)
from middleware.helpers import (
    log_and_suppress,
    get_or_default,
    log_and_suppress_decorator,
)


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
        config["pooling_interval"] = get_or_default(config, "pooling_interval", 2)
        config["retries_count"] = get_or_default(config, "retries_count", 5)
        config["max_prefetch_range"] = get_or_default(config, "max_prefetch_range", 20)
        config["max_allowed_range"] = get_or_default(config, "max_allowed_range", 100)
        config["prefetch"] = get_or_default(config, "prefetch", [])

        super().__init__(alias, config)

        self.__block_number = 0
        self.__optimizations_table = {
            # Ethereum API
            "eth_accounts": self.__optimize_eth_accounts,
            "eth_chainId": self.__optimize_eth_chain_id,
            "eth_blockNumber": self.__optimize_eth_block_number,
            "eth_getBlockByNumber": self.__optimize_eth_get_block_by_number,
            "eth_getLogs": self.__optimize_eth_get_logs,
            "eth_sendRawTransaction": self.__optimize_eth_send_raw_transaction,
            # Trace API
            "trace_block": self.__optimize_trace_block,
        }

    def __repr__(self) -> str:
        return f"Optimizer({self.alias}, {self.config})"

    def __str__(self) -> str:
        return f"Optimizer({self.alias})"

    async def _handle_request(self, request: Message) -> Message:
        if not self.__block_number:
            raise Exception("Optimizer not ready")
        method = request.as_json("method")
        if method in self.__optimizations_table:
            return await self.__optimizations_table[method](request)
        return await super()._handle_request(request)

    def _on_application_setup(self, app: web.Application):
        app.cleanup_ctx.append(self.__ctx_cleanup)

    async def __ctx_cleanup(self, _):
        self.__block_available = Event()
        tasks = [
            asyncio.create_task(self.__prefetch_block_number()),
        ]
        if self.config["prefetch"]:
            tasks.append(
                asyncio.create_task(self.__prefetch_data()),
            )

        yield

        for task in tasks:
            task.cancel()
        with suppress(asyncio.CancelledError):
            await asyncio.gather(*tasks)

    async def __fetch_block_number_with_retries(self) -> int:
        msg = set_no_cache_tag(make_request_message("eth_blockNumber"))
        for _ in range(self.config["retries_count"]):
            with log_and_suppress(logger, f"prefetch-block-number - {msg}"):
                response = await super()._handle_request(msg)
                new_block_number = int(response.as_json("result"), 0)
                if new_block_number >= self.__block_number:
                    return new_block_number
        return self.__block_number

    @log_and_suppress_decorator(logger)
    async def __prefetch_data_common(self, msg: Message):
        for _ in range(self.config["retries_count"]):
            if is_response_success(await super()._handle_request(msg)):
                return

    async def __prefetch_block_number(self):
        while True:
            new_block_number = await self.__fetch_block_number_with_retries()
            if new_block_number > self.__block_number:
                self.__block_number = new_block_number
                self.__block_available.set()
            await asyncio.sleep(self.config["pooling_interval"])

    async def __prefetch_data(self):
        last_fetched_block = self.__block_number
        while True:
            await self.__block_available.wait()
            self.__block_available.clear()

            if (
                self.__block_number - last_fetched_block
                > self.config["max_prefetch_range"]
            ):
                # Fast forward to present block
                last_fetched_block = self.__block_number - 1

            # Trigger prefetch in the range (last_fetched_block, self.__block_number]
            tasks = []
            for block_number in range(last_fetched_block + 1, self.__block_number + 1):
                for request_builder in self.config["prefetch"]:
                    tasks.append(
                        self.__prefetch_data_common(request_builder(block_number))
                    )
            last_fetched_block = self.__block_number

            await asyncio.gather(*tasks)

    async def __optimize_eth_accounts(self, _: Message) -> Message:
        return make_message_with_result()

    async def __optimize_eth_block_number(self, _: Message) -> Message:
        return make_message_with_result(hex(self.__block_number))

    async def __optimize_eth_chain_id(self, request: Message) -> Message:
        return await self.__optimize_request_without_params(request)

    async def __optimize_eth_get_block_by_number(self, request: Message) -> Message:
        return await self.__optimize_request_with_tag_in_params(request, 0)

    async def __optimize_eth_get_logs(self, request: Message) -> Message:
        do_not_cache = False
        # Normalize request for better cache use
        request_obj = request.as_json()
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
                if _from - _to > self.config["max_allowed_range"]:
                    raise EthLimitExceeded(
                        f"{request_obj['method']}: max allowed range is {self.config['max_allowed_range']}"
                    )
                do_not_cache = True
            if _from > self.__block_number:
                # Optimization: return null result for block in the future
                return make_message_with_result()
        if "topics" in original_args and original_args["topics"]:
            normalized_args["topics"] = original_args["topics"]
            do_not_cache = True

        optimized_request = make_request_message(
            request_obj["method"], [normalized_args]
        )
        if do_not_cache:
            optimized_request = set_no_cache_tag(optimized_request)
        return await super()._handle_request(optimized_request)

    async def __optimize_trace_block(self, request: Message) -> Message:
        return await self.__optimize_request_with_tag_in_params(request, 0)

    async def __optimize_eth_send_raw_transaction(self, request: Message) -> Message:
        return await super()._handle_request(set_no_cache_tag(request))

    async def __optimize_request_without_params(self, request: Message) -> Message:
        return await super()._handle_request(
            make_request_message(request.as_json("method"))
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
