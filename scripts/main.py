#!/usr/bin/env python3

"""
Proxy for ethereum compatible chains.
"""

import gc
import logging
import os
import sys
import json

from aiohttp import web

from extensions.abstract.extension_base import Extension
from extensions.abstract.extension_base import get_extension_by_name
from utils.helpers import get_or_default


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


def setup_extension(app: web.Application, ext: Extension, prefix: str):
    app.add_routes([route for route in ext.do_get_routes(prefix)])
    app.cleanup_ctx.append(ext.ctx_cleanup)
    app.on_shutdown.append(ext.on_shutdown)


def main():
    app = web.Application()

    config = {}
    with open(sys.argv[1]) as input:
        config = json.load(input)

    # Prepare execution flows
    extensions = {}
    for ext_alias in config["extensions"]:
        extensions[ext_alias] = get_extension_by_name(
            config["extensions"][ext_alias]["type"]
        )(ext_alias, get_or_default(config["extensions"][ext_alias], "config", {}))
        setup_extension(app, extensions[ext_alias], f"/{ext_alias}")
    for grp in config["flows"]:
        for ext_alias in config["flows"][grp]:
            for next_handler in config["flows"][grp][ext_alias]:
                extensions[ext_alias].do_add_next_handler(extensions[next_handler])

    web.run_app(app=app)

    # Prevents exceptions during shutdown
    gc.collect()


if __name__ == "__main__":
    main()
