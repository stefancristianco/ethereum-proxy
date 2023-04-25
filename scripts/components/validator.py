"""
Validate requests.
"""

import logging
import os

from components.abstract.round_robin_selector import RoundRobinSelector
from middleware.message import (
    Message,
    EthInvalidRequest,
    EthJsonVersion,
    EthMethodNotFound,
    EthInvalidParams,
    EthNotSupported,
)


#
# Setup logger
#

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

logger.setLevel(os.environ.setdefault("LOG_LEVEL", "INFO"))


class Validator(RoundRobinSelector):
    def __init__(self, alias: str, config: dict):
        super().__init__(alias, config)

        self.__validator_table = {
            # Ethereum API
            "eth_accounts": self.__handle_eth_accounts,
            "eth_blockNumber": self.__handle_eth_block_number,
            "eth_call": self.__handle_allow_call,
            "eth_chainId": self.__handle_eth_chain_id,
            "eth_estimateGas": self.__handle_allow_call,
            "eth_feeHistory": self.__handle_allow_call,
            "eth_gasPrice": self.__handle_allow_call,
            "eth_getBalance": self.__handle_eth_get_balance,
            "eth_getBlockByHash": self.__handle_allow_call,
            "eth_getBlockByNumber": self.__handle_eth_get_block_by_number,
            "eth_getBlockTransactionCountByHash": self.__handle_allow_call,
            "eth_getBlockTransactionCountByNumber": self.__handle_allow_call,
            "eth_getCode": self.__handle_eth_get_code,
            "eth_getLogs": self.__handle_eth_get_logs,
            "eth_getProof": self.__handle_allow_call,
            "eth_getStorageAt": self.__handle_eth_get_storage_at,
            "eth_getTransactionByBlockHashAndIndex": self.__handle_allow_call,
            "eth_getTransactionByBlockNumberAndIndex": self.__handle_eth_get_transaction_by_block_number_and_index,
            "eth_getTransactionByHash": self.__handle_allow_call,
            "eth_getTransactionCount": self.__handle_eth_get_transaction_count,
            "eth_getTransactionReceipt": self.__handle_eth_get_transaction_receipt,
            "eth_getUncleByBlockHashAndIndex": self.__handle_allow_call,
            "eth_getUncleByBlockNumberAndIndex": self.__handle_eth_get_uncle_by_block_number_and_index,
            "eth_getUncleCountByBlockHash": self.__handle_allow_call,
            "eth_getUncleCountByBlockNumber": self.__handle_eth_get_uncle_count_by_block_number,
            "eth_maxPriorityFeePerGas": self.__handle_allow_call,
            "eth_sendRawTransaction": self.__handle_allow_call,
            "net_listening": self.__handle_allow_call,
            "net_peerCount": self.__handle_allow_call,
            "net_version": self.__handle_allow_call,
            "web3_clientVersion": self.__handle_allow_call,
            "web3_sha3": self.__handle_allow_call,
            # Trace API
            "trace_block": self.__handle_trace_block,
            "trace_call": self.__handle_trace_call,
            "trace_callMany": self.__handle_trace_call_many,
            "trace_filter": self.__handle_trace_filter,
            "trace_get": self.__handle_allow_call,
            "trace_replayBlockTransactions": self.__handle_replay_block_transactions,
            "trace_replayTransaction": self.__handle_allow_call,
            "trace_transaction": self.__handle_allow_call,
        }

    def __repr__(self) -> str:
        return f"Validator({self.get_alias()}, {self.get_config()})"

    def __str__(self) -> str:
        return f"Validator({self.get_alias()})"

    async def _handle_request(self, request: Message) -> Message:
        request_obj = request.as_json()
        if isinstance(request_obj, list):
            raise EthNotSupported("Batch requests not allowed")
        for key in ["jsonrpc", "id", "method"]:
            if not key in request_obj:
                raise EthInvalidRequest(f"Missing '{key}' entry")
        if request_obj["jsonrpc"] != "2.0":
            raise EthJsonVersion("Expecting jsonrpc version 2.0")
        if request_obj["method"] in self.__validator_table:
            return await self.__validator_table[request_obj["method"]](request)
        raise EthMethodNotFound(f"{request_obj['method']} not allowed")

    async def __handle_allow_call(self, request: Message) -> Message:
        return self.__validate_response(await super()._handle_request(request))

    async def __handle_eth_accounts(self, request: Message) -> Message:
        self.__ensure_empty_array_params(request)
        return await self.__handle_allow_call(request)

    async def __handle_eth_chain_id(self, request: Message) -> Message:
        self.__ensure_empty_array_params(request)
        return await self.__handle_allow_call(request)

    async def __handle_eth_block_number(self, request: Message) -> Message:
        self.__ensure_empty_array_params(request)
        return await self.__handle_allow_call(request)

    async def __handle_eth_get_block_by_number(self, request: Message) -> Message:
        self.__ensure_array_params_with_size(request, 2)
        self.__ensure_array_params_with_types(request, [str, bool])
        return await self.__handle_allow_call(request)

    async def __handle_eth_get_code(self, request: Message) -> Message:
        self.__ensure_array_params_with_size(request, 2)
        self.__ensure_array_params_with_types(request, [str, str])
        return await self.__handle_allow_call(request)

    async def __handle_eth_get_logs(self, request: Message) -> Message:
        self.__ensure_array_params_with_size(request, 1)
        self.__ensure_array_params_with_types(request, [dict])
        return await self.__handle_allow_call(request)

    async def __handle_eth_get_transaction_receipt(self, request: Message) -> Message:
        self.__ensure_array_params_with_size(request, 1)
        self.__ensure_array_params_with_types(request, [str])
        return await self.__handle_allow_call(request)

    async def __handle_eth_get_balance(self, request: Message) -> Message:
        self.__ensure_array_params_with_size(request, 2)
        self.__ensure_array_params_with_types(request, [str, str])
        return await self.__handle_allow_call(request)

    async def __handle_eth_get_storage_at(self, request: Message) -> Message:
        self.__ensure_array_params_with_size(request, 3)
        self.__ensure_array_params_with_types(request, [str, str, str])
        return await self.__handle_allow_call(request)

    async def __handle_eth_get_transaction_by_block_number_and_index(
        self, request: Message
    ) -> Message:
        self.__ensure_array_params_with_size(request, 2)
        self.__ensure_array_params_with_types(request, [str, str])
        return await self.__handle_allow_call(request)

    async def __handle_eth_get_transaction_by_block_number_and_index(
        self, request: Message
    ) -> Message:
        self.__ensure_array_params_with_size(request, 2)
        self.__ensure_array_params_with_types(request, [str, str])
        return await self.__handle_allow_call(request)

    async def __handle_eth_get_transaction_count(self, request: Message) -> Message:
        self.__ensure_array_params_with_size(request, 2)
        self.__ensure_array_params_with_types(request, [str, str])
        return await self.__handle_allow_call(request)

    async def __handle_eth_get_uncle_by_block_number_and_index(
        self, request: Message
    ) -> Message:
        self.__ensure_array_params_with_size(request, 2)
        self.__ensure_array_params_with_types(request, [str, str])
        return await self.__handle_allow_call(request)

    async def __handle_eth_get_uncle_count_by_block_number(
        self, request: Message
    ) -> Message:
        self.__ensure_array_params_with_size(request, 1)
        self.__ensure_array_params_with_types(request, [str])
        return await self.__handle_allow_call(request)

    async def __handle_trace_block(self, request: Message) -> Message:
        self.__ensure_array_params_with_size(request, 1)
        self.__ensure_array_params_with_types(request, [str])
        return await self.__handle_allow_call(request)

    async def __handle_trace_call(self, request: Message) -> Message:
        self.__ensure_array_params_with_size(request, 2)
        self.__ensure_array_params_with_types(request, [dict, list])
        return await self.__handle_allow_call(request)

    async def __handle_trace_call_many(self, request: Message) -> Message:
        self.__ensure_array_params_with_size(request, 1)
        self.__ensure_array_params_with_types(request, [list])
        return await self.__handle_allow_call(request)

    async def __handle_trace_filter(self, request: Message) -> Message:
        self.__ensure_array_params_with_size(request, 1)
        self.__ensure_array_params_with_types(request, [dict])
        return await self.__handle_allow_call(request)

    async def __handle_replay_block_transactions(self, request: Message) -> Message:
        self.__ensure_array_params_with_size(request, 2)
        self.__ensure_array_params_with_types(request, [str, list])
        return await self.__handle_allow_call(request)

    def __ensure_empty_array_params(self, request: Message):
        request_obj = request.as_json()
        if "params" in request_obj:
            if not isinstance(request_obj["params"], list):
                raise EthInvalidParams(f"{request_obj['method']}: wrong params type")
            if len(request_obj["params"]):
                raise EthInvalidParams(f"{request_obj['method']}: too many arguments")

    def __ensure_array_params_with_size(self, request: Message, size: int):
        request_obj = request.as_json()
        if not "params" in request_obj:
            raise EthInvalidParams(f"{request_obj['method']}: missing params")
        if not isinstance(request_obj["params"], list):
            raise EthInvalidParams(f"{request_obj['method']}: wrong params type")
        if len(request_obj["params"]) < size:
            raise EthInvalidParams(
                f"{request_obj['method']}: expected {size} params, but found {len(request_obj['params'])}"
            )

    def __ensure_array_params_with_types(self, request: Message, types: list):
        request_obj = request.as_json()
        params = request_obj["params"]

        assert len(params) >= len(types)

        for index in range(len(types)):
            if not isinstance(params[index], types[index]):
                raise EthInvalidParams(
                    f"{request_obj['method']}: invalid param[{index}] type"
                )

    def __validate_response(self, msg: Message) -> Message:
        # TODO deactivated until further investigations
        # if len(msg) > 512:
        #     # Optimization: error messages are small in size
        #     return msg
        # msg_obj = msg.as_json()
        # if "error" in msg_obj:
        #     error_obj = msg_obj["error"]
        #     logger.error(f"Error response: {error_obj}")
        #     if not "code" in error_obj:
        #         # Non standard error format
        #         raise EthInternalError()
        #     # Hide error source to external client
        #     raise EthException(int(error_obj["code"]))
        return msg
