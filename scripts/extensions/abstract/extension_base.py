"""
Every extension should derive this class and implement the required functionality.
"""

import logging
import os

from abc import ABC
from abc import abstractmethod

from utils.message import Message
from utils.helpers import unreachable_code

#
# Setup logger
#

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

logger.setLevel(os.environ.setdefault("LOG_LEVEL", "INFO"))


#
# Extension logic and interfaces
#


class ExtensionBase(ABC):
    """Extension base required functionality"""

    def __init__(self, alias: str, config: dict):
        self.__alias = alias
        self.__config = config

    def get_alias(self) -> str:
        return self.__alias

    def get_config(self):
        return self.__config

    async def do_handle_request(self, request: Message) -> Message:
        return await self._handle_request(request)

    async def ctx_cleanup(self, _):
        yield

    async def on_shutdown(self, _):
        pass

    @abstractmethod
    async def _handle_request(self, request: Message) -> Message:
        pass


class Extension(ExtensionBase):
    def __init__(self, alias: str, config: dict):
        super().__init__(alias, config)

    def do_add_next_handler(self, next_handler: ExtensionBase):
        logger.debug(f"Connecting {self} -> {next_handler}")
        self._add_next_handler(next_handler)

    def do_get_routes(self, prefix: str) -> list:
        return self._get_routes(prefix)

    def _add_next_handler(self, _: ExtensionBase):
        unreachable_code()

    def _get_routes(self, _: str) -> list:
        return []


def get_extension_by_name(ext_name: str) -> Extension:
    """Load extension module.
    :param ext_name: the name of the extension to load (e.g. "lb").
    :return: the extension instance if successful, throws exception otherwise.
    """
    importlib = __import__("importlib")
    extensions = importlib.import_module(f"extensions.{ext_name}")
    return getattr(
        extensions, "".join(part.capitalize() for part in ext_name.split("_"))
    )
