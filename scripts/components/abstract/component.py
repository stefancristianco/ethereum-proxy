import logging
import os

from abc import abstractmethod

from middleware.abstract.component_base import ComponentBase
from middleware.message import Message

from middleware.helpers import concat

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
        logger.debug(f"{alias=} {config=}")

    def do_add_next_handler(self, next_handler: Component):
        self._add_next_handler(next_handler)
        logger.debug(f"Connecting {self} -> {next_handler}")

    @abstractmethod
    def _add_next_handler(self, _: Component):
        pass


def get_component_by_name(comp_name: str) -> ComponentLink:
    """Load component module.
    :param comp_name: the name of the component to load (e.g. "lb").
    :return: the component instance if successful, throws exception otherwise.
    """
    importlib = __import__("importlib")
    extensions = importlib.import_module(f"components.{comp_name}")
    return getattr(
        extensions, concat(part.capitalize() for part in comp_name.split("_"))
    )
