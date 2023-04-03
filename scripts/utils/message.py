import json

from typing import Any


RESTRICTED_KEYS = {"__data__", "__json__", "__hash__"}


class Message:
    def __init__(self, data: Any):
        assert isinstance(data, str) or isinstance(data, bytes)
        self.__attachments = {"__data__": data}

    def __repr__(self) -> str:
        return f"Message({self.__attachments['__data__']})"

    def retrieve(self, key: str) -> Any:
        assert key not in RESTRICTED_KEYS
        return self.__attachments[key]

    def attach(self, key: str, value: Any) -> None:
        assert key not in RESTRICTED_KEYS
        self.__attachments[key] = value

    def as_json(self) -> Any:
        if not "__json__" in self.__attachments:
            self.__attachments["__json__"] = json.loads(self.__attachments["__data__"])
        return self.__attachments["__json__"]

    def as_raw_data(self) -> Any:
        return self.__attachments["__data__"]

    def __len__(self):
        return len(self.__attachments["__data__"])

    def __hash__(self) -> int:
        """Produce a hash from the message body.
        The variable part is removed firs (e.g. {'id': xxx}).
        """
        if not "__hash__" in self.__attachments:
            adjusted_message = dict(self.as_json())
            del adjusted_message["id"]
            self.__attachments["__hash__"] = hash(json.dumps(adjusted_message))
        return self.__attachments["__hash__"]
