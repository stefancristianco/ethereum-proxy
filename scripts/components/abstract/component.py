import logging
import os

from abc import abstractmethod

from middleware.abstract.component_base import ComponentBase
from middleware.message import Message

#
# Setup logger
#

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

logger.setLevel(os.environ.setdefault("LOG_LEVEL", "INFO"))


#
# Component logic and interfaces
#


class Component(ComponentBase):
    """Component required functionality"""

    def __init__(self, alias: str, config: dict):
        super().__init__(alias, config)

    async def do_handle_request(self, request: Message) -> Message:
        return await self._handle_request(request)

    @abstractmethod
    async def _handle_request(self, request: Message) -> Message:
        pass


class ComponentLink(Component):
    def __init__(self, alias: str, config: dict):
        super().__init__(alias, config)

    def do_add_next_handler(self, next_handler: Component):
        logger.debug(f"Connecting {self} -> {next_handler}")
        self._add_next_handler(next_handler)

    @abstractmethod
    def _add_next_handler(self, _: Component):
        pass


def get_component_by_name(ext_name: str) -> ComponentLink:
    """Load component module.
    :param ext_name: the name of the component to load (e.g. "lb").
    :return: the component instance if successful, throws exception otherwise.
    """
    importlib = __import__("importlib")
    extensions = importlib.import_module(f"components.{ext_name}")
    return getattr(
        extensions, "".join(part.capitalize() for part in ext_name.split("_"))
    )
