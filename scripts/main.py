#!/usr/bin/env python3

import gc
import logging
import os
import sys

from aiohttp import web

from components.abstract.component import get_component_by_name
from middleware.helpers import concat, get_or_default


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
    logger.error("Usage: python main.py config_name")
    sys.exit(-1)


logger.info(sys.argv)
if len(sys.argv) != 2:
    show_usage_and_exit()


def get_config_by_name(config_name: str):
    """Load configuration module.
    :param config_name: the name of the config module to load (e.g. "module_name").
    :return: the config instance if successful, throws exception otherwise.
    """
    importlib = __import__("importlib")
    module = importlib.import_module(f"config.{config_name}")
    clazz = getattr(
        module, concat(part.capitalize() for part in config_name.split("_"))
    )
    return clazz().config()


def main():
    config = get_config_by_name(sys.argv[1])

    app = web.Application()

    # Prepare execution flows
    components = {}
    for alias in config["components"]:
        components[alias] = get_component_by_name(config["components"][alias]["type"])(
            alias, get_or_default(config["components"][alias], "config", {})
        )
        components[alias].do_setup_application(app)
    for grp in config["flows"]:
        for alias in config["flows"][grp]:
            for next_handler in config["flows"][grp][alias]:
                components[alias].do_add_next_handler(components[next_handler])

    web.run_app(app=app)

    # Prevents exceptions during shutdown
    gc.collect()


if __name__ == "__main__":
    main()
