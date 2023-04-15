"""
Every extension should derive this class and implement the required functionality.
"""

import logging
import os

from abc import ABC
from abc import abstractmethod

from utils.message import Message

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

    def __init__(self, name: str):
        self.__name = name

    def get_name(self) -> str:
        return self.__name

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
    def __init__(self, name: str):
        super().__init__(name)

    def do_add_next_handler(self, next_handler: ExtensionBase):
        logger.debug(f"Connecting {self} -> {next_handler}")
        self._add_next_handler(next_handler)

    def do_get_routes(self) -> list:
        return self._get_routes()

    @abstractmethod
    def _add_next_handler(self, next_handler: ExtensionBase):
        pass

    def _get_routes(self) -> list:
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
