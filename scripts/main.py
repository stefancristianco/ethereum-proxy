#!/usr/bin/env python3

"""
Proxy for ethereum compatible chains.
"""

import gc
import logging
import argparse
import os

from aiohttp import web

from extensions.receiver import Receiver
from extensions.endpoint import Endpoint
from extensions.abstract.extension_base import Extension

from extensions.abstract.extension_base import get_extension_by_name


#
# Read cmd line arguments
#

parser = argparse.ArgumentParser(
    prog="main.py",
    description="Starts a load balancer for ethereum compatible nodes",
    epilog="Work in progress, use at own risk!",
)


def add_multiarg_option(flag: str, required: bool, metavar: str, help: str):
    parser.add_argument(
        flag, required=required, metavar=metavar, type=str, nargs="+", help=help
    )


add_multiarg_option("--extensions", False, "EXTENSION", "list of extensions to use")
add_multiarg_option("--pool", True, "URL", "url address for end-point")

args = parser.parse_args()

#
# Setup logger
#

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

logger.setLevel(os.environ.setdefault("LOG_LEVEL", "INFO"))


#
# Load extensions
#

logger.info("====== Using Extensions ======")
logger.info(args.extensions)

extensions = []
if args.extensions:
    extensions = [get_extension_by_name(ext_name)() for ext_name in args.extensions]

#
# Initialize endpoints
#

logger.info("======== Pool entries ========")
for url in args.pool:
    logger.info(url)


def setup_extension(app: web.Application, ext: Extension):
    app.add_routes([route for route in ext.do_get_routes()])
    app.cleanup_ctx.append(ext.ctx_cleanup)
    app.on_shutdown.append(ext.on_shutdown)


def main():
    app = web.Application()

    # Prepare pipeline structure
    pipeline = [[Receiver()]]
    for ext in extensions:
        pipeline.append([ext])
    pipeline.append([Endpoint(url) for url in args.pool])

    prev_ext_grp = []
    for ext_grp in pipeline:
        for ext in ext_grp:
            setup_extension(app, ext)
            for prev_ext in prev_ext_grp:
                prev_ext.do_add_next_handler(ext)
        prev_ext_grp = ext_grp

    web.run_app(app=app)

    # Prevents exceptions during shutdown
    gc.collect()


if __name__ == "__main__":
    main()
