from abc import ABC
from abc import abstractmethod

from aiohttp import web


class ConfigBase(ABC):
    def __init__(self, alias: str, config: dict):
        self.__alias = alias
        self.__config = config

    def get_alias(self) -> str:
        return self.__alias

    def get_config(self, key: str = None):
        if key:
            return self.__config[key]
        return self.__config

    def private_key(self, key: str) -> str:
        return f"{self.__alias}__{key}"


class ComponentBase(ConfigBase):
    def __init__(self, alias: str, config: dict):
        super().__init__(alias, config)

    def do_setup_application(self, app: web.Application):
        self._on_application_setup(app)

    @abstractmethod
    def _on_application_setup(self, app: web.Application):
        pass
