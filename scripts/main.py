#!/usr/bin/env python3

import gc
import importlib
import functools
import logging
import os
import sys

from aiohttp import web

from middleware.listeners import HttpListener, WsListener

from components.abstract.component import get_component_by_name
from middleware.helpers import concat, get_or_default, extract_entrypoints


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
    logger.error("Usage: python scripts/main.py config_name")
    sys.exit(-1)


logger.info(sys.argv)
if len(sys.argv) != 2:
    show_usage_and_exit()


def get_config_by_name(config_name: str):
    """
    Dynamically loads a configuration module and returns an instance of the configuration.

    Args:
        config_name (str): The name of the configuration module to load (e.g., "module_name").

    Returns:
        object: An instance of the configuration class if successful.

    Raises:
        ImportError: If the configuration module cannot be found or loaded.
        AttributeError: If the configuration class cannot be found in the module.
    """
    module = importlib.import_module(f"configs.{config_name}")
    clazz = getattr(
        module, concat(part.capitalize() for part in config_name.split("_"))
    )
    return clazz().config()


def setup_routes(app, clazz, inst, alias, config):
    """
    Sets up HTTP and WebSocket routes for each method marked with the "entrypoint" decorator.

    Args:
        app (aiohttp.web.Application): The application instance to add routes to.
        clazz (type): The class type to extract entrypoints from.
        inst (object): An instance of the class.
        alias (str): The alias for the routes.
        config: Additional configuration data.

    Usage:
        class MyClass:
            @entrypoint
            def my_method(self, msg: Message) -> Message:
                # Entry point method implementation

        # Set up routes for MyClass entrypoints
        setup_routes(app, MyClass, MyClass(), "myalias", config)
    """
    for key, value in extract_entrypoints(clazz):
        http_route = f"/{alias}/{key}/"
        ws_route = f"/{alias}/{key}/ws"

        logger.info(f"{inst}: http://...{http_route}")
        logger.info(f"{inst}: ws://...{ws_route}")

        callback = functools.partial(value, inst)
        http_listener = HttpListener(key, config, callback)
        ws_listener = WsListener(key, config, callback)
        app.add_routes(
            [
                web.post(http_route, http_listener.handle_request),
                web.get(ws_route, ws_listener.handle_request),
            ]
        )


def main():
    config = get_config_by_name(sys.argv[1])

    app = web.Application()

    # Prepare execution flows
    components = {}
    for alias in config["components"]:
        clazz = get_component_by_name(config["components"][alias]["type"])
        comp_config = get_or_default(config["components"][alias], "config", {})
        comp_inst = clazz(alias, comp_config)
        comp_inst.do_setup_application(app)
        setup_routes(app, clazz, comp_inst, alias, comp_config)
        components[alias] = comp_inst
        logger.info(f"{comp_inst}: {comp_config}")

    for grp in config["flows"]:
        for alias in config["flows"][grp]:
            for next_handler in config["flows"][grp][alias]:
                components[alias].do_add_next_handler(components[next_handler])

    web.run_app(app=app)

    # Prevents exceptions during shutdown
    gc.collect()


if __name__ == "__main__":
    main()
