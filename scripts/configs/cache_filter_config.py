ref_cache_filters = {
    "medium_duration_filter": [
        # Ethereum API
        "eth_getBlockByHash",
        "eth_getBlockByNumber",
        "eth_getBlockTransactionCountByHash",
        "eth_getBlockTransactionCountByNumber",
        "eth_getCode",
        "eth_getLogs",
        # Trace API
        "trace_block",
    ],
    "long_duration_filter": [
        "eth_chainId",
        "net_listening",
        "net_peerCount",
        "net_version",
        "web3_clientVersion",
        "web3_sha3",
    ],
}
