from abc import ABC
from abc import abstractmethod


class ConfigBase(ABC):
    @abstractmethod
    def config(self) -> dict:
        pass


def get_or_default(config: dict, key: str, default):
    if not key in config:
        return default
    assert isinstance(config, dict)
    assert isinstance(config[key], type(default))
    return config[key]
