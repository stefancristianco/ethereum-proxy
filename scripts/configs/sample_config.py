from configs.cache_filter_config import ref_cache_filters

from configs.request_templates import eth_get_block_by_number, eth_get_logs, trace_block


class SampleConfig:
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
                            eth_get_logs,
                            eth_get_block_by_number,
                            trace_block,
                        ],
                    },
                },
                "cache": {"type": "cache", "config": {**ref_cache_filters}},
                "lb": {
                    "type": "load_balancer",
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
                "endpoint-6": {
                    "type": "endpoint",
                    "config": {
                        "url": "wss://eth-mainnet.public.blastapi.io",
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
                        "endpoint-6",
                    ],
                }
            },
        }
