#!/usr/bin/env python3

import gc
import logging
import os
import sys
import json

from aiohttp import web

from components.abstract.component import get_component_by_name
from middleware.helpers import get_or_default


#
# Setup logger
#


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

logger.setLevel(os.environ.setdefault("LOG_LEVEL", "INFO"))


#
# Read cmd line arguments
#


def show_usage_and_exit():
    logger.error("Usage: python main.py /path/to/config.json")
    sys.exit(-1)


logger.info(sys.argv)
if len(sys.argv) != 2:
    show_usage_and_exit()


def enhance_component_config(config: dict, ext_alias: str) -> dict:
    ext_config = get_or_default(config["components"][ext_alias], "config", {})
    if "refs" in ext_config:
        assert "common-refs" in config
        for ref in ext_config["refs"]:
            assert ref in config["common-refs"]
            # Only fill missing entries
            for key in config["common-refs"][ref]:
                if not key in ext_config:
                    ext_config[key] = config["common-refs"][ref][key]
        del ext_config["refs"]
    logger.debug(f"{ext_alias=}: {ext_config=}")
    return ext_config


def main():
    app = web.Application()

    config = {}
    with open(sys.argv[1]) as input:
        config = json.load(input)

    # Prepare execution flows
    components = {}
    for ext_alias in config["components"]:
        components[ext_alias] = get_component_by_name(
            config["components"][ext_alias]["type"]
        )(ext_alias, enhance_component_config(config, ext_alias))
        components[ext_alias].do_setup_application(app)
    for grp in config["flows"]:
        for ext_alias in config["flows"][grp]:
            for next_handler in config["flows"][grp][ext_alias]:
                components[ext_alias].do_add_next_handler(components[next_handler])

    web.run_app(app=app)

    # Prevents exceptions during shutdown
    gc.collect()


if __name__ == "__main__":
    main()
