from middleware.abstract.config_base import ConfigBase

from config.lb_blacklist_config import ref_blacklist_filters
from config.cache_filter_config import ref_cache_filters


class SampleConfig(ConfigBase):
    def config(self) -> dict:
        ref_sample = {"tasks_count": 2}
        return {
            "components": {
                "receiver": {"type": "receiver"},
                "validator": {"type": "validator"},
                "optimizer": {
                    "type": "optimizer",
                    "config": {
                        "pooling_interval": 3,
                        "prefetch": [
                            "eth_getLogs",
                            "eth_getBlockByNumber",
                            "trace_block",
                        ],
                    },
                },
                "cache": {"type": "cache", "config": {**ref_cache_filters}},
                "lb": {
                    "type": "load_balancer",
                    "config": {**ref_blacklist_filters},
                },
                "endpoint-1": {
                    "type": "endpoint",
                    "config": {"url": "https://rpc.ankr.com/eth", **ref_sample},
                },
                "endpoint-2": {
                    "type": "endpoint",
                    "config": {
                        "url": "https://ethereum-mainnet-rpc.allthatnode.com",
                        **ref_sample,
                    },
                },
                "endpoint-3": {
                    "type": "endpoint",
                    "config": {"url": "https://cloudflare-eth.com", **ref_sample},
                },
                "endpoint-4": {
                    "type": "endpoint",
                    "config": {"url": "https://rpc.flashbots.net", **ref_sample},
                },
                "endpoint-5": {
                    "type": "endpoint",
                    "config": {
                        "url": "https://eth-rpc.gateway.pokt.network",
                        **ref_sample,
                    },
                },
                "debug": {"type": "debug"},
            },
            "flows": {
                "proxy": {
                    "receiver": ["validator"],
                    "validator": ["optimizer"],
                    "optimizer": ["cache"],
                    "debug": ["cache"],
                    "cache": ["lb"],
                    "lb": [
                        "endpoint-1",
                        "endpoint-2",
                        "endpoint-3",
                        "endpoint-4",
                        "endpoint-5",
                    ],
                }
            },
        }
