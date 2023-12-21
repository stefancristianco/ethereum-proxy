from middleware.helpers import printable_function
from middleware.message import make_request_message


@printable_function
def eth_get_logs(block_number: int):
    return make_request_message(
        "eth_getLogs",
        [
            {
                "fromBlock": hex(block_number),
                "toBlock": hex(block_number),
            }
        ],
    )


@printable_function
def eth_get_block_by_number(block_number: int):
    return make_request_message("eth_getBlockByNumber", [hex(block_number), True])


@printable_function
def trace_block(block_number: int):
    return make_request_message("trace_block", [hex(block_number)])
