import json
import copy

MESSAGE_HEADER = {"jsonrpc": "2.0", "id": 0}
RESTRICTED_KEYS = {"__data__", "__json__", "__hash__"}
ETH_ERROR_CODES = {
    -32700: "Parse error",  #                    # Invalid JSON                                      (standard)
    -32600: "Invalid request",  #                # JSON is not a valid request object                (standard)
    -32601: "Method not found",  #               # Method does not exist                             (standard)
    -32602: "Invalid params",  #                 # Invalid method parameters                         (standard)
    -32603: "Internal error",  #                 # Internal JSON-RPC error                           (standard)
    -32000: "Invalid input",  #                  # Missing or invalid parameters                     (non-standard)
    -32001: "Resource not found",  #             # Requested resource not found                      (non-standard)
    -32002: "Resource unavailable",  #           # Requested resource not available                  (non-standard)
    -32003: "Transaction rejected",  #           # Transaction creation failed                       (non-standard)
    -32004: "Method not supported",  #           # Method is not implemented                         (non-standard)
    -32005: "Limit exceeded",  #                 # Request exceeds defined limit                     (non-standard)
    -32006: "JSON-RPC version not supported",  # # Version of JSON-RPC protocol is not supported     (non-standard)
    -32050: "Not supported",  #                  # Request not supported by proxy                    (custom)
}


class Message:
    def __init__(self, data):
        assert isinstance(data, str) or isinstance(data, bytes)
        self.__attachments = {"__data__": data}

    def __repr__(self) -> str:
        return f"Message({self.__attachments['__data__']})"

    def retrieve(self, key: str):
        assert key not in RESTRICTED_KEYS
        return self.__attachments[key]

    def attach(self, key: str, value):
        assert key not in RESTRICTED_KEYS
        self.__attachments[key] = value

    def as_json(self):
        if not "__json__" in self.__attachments:
            self.__attachments["__json__"] = json.loads(self.__attachments["__data__"])
        return self.__attachments["__json__"]

    def as_json_copy(self):
        if not "__json__" in self.__attachments:
            self.__attachments["__json__"] = json.loads(self.__attachments["__data__"])
        return copy.deepcopy(self.__attachments["__json__"])

    def as_raw_data(self):
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
            del adjusted_message["jsonrpc"]
            self.__attachments["__hash__"] = hash(json.dumps(adjusted_message))
        return self.__attachments["__hash__"]


class EthException(Exception):
    def __init__(self, code: int, data: str = None):
        assert code in ETH_ERROR_CODES
        self.__code = code
        self.__data = data

    def as_json(self):
        return {
            "error": {
                "code": self.__code,
                "message": ETH_ERROR_CODES[self.__code],
                "data": self.__data,
            }
        }


class EthParseError(EthException):
    def __init__(self, data: str = None):
        super().__init__(-32700, data)


class EthInvalidRequest(EthException):
    def __init__(self, data: str = None):
        super().__init__(-32600, data)


class EthMethodNotFound(EthException):
    def __init__(self, data: str = None):
        super().__init__(-32601, data)


class EthInvalidParams(EthException):
    def __init__(self, data: str = None):
        super().__init__(-32602, data)


class EthInternalError(EthException):
    def __init__(self, data: str = None):
        super().__init__(-32603, data)


class EthInvalidInput(EthException):
    def __init__(self, data: str = None):
        super().__init__(-32000, data)


class EthResourceNotFound(EthException):
    def __init__(self, data: str = None):
        super().__init__(-32001, data)


class EthResourceUnavailable(EthException):
    def __init__(self, data: str = None):
        super().__init__(-32002, data)


class EthTransactionRejected(EthException):
    def __init__(self, data: str = None):
        super().__init__(-32003, data)


class EthMethodNotSupported(EthException):
    def __init__(self, data: str = None):
        super().__init__(-32004, data)


class EthLimitExceeded(EthException):
    def __init__(self, data: str = None):
        super().__init__(-32004, data)


class EthJsonVersion(EthException):
    def __init__(self, data: str = None):
        super().__init__(-32006, data)


class EthNotSupported(EthException):
    def __init__(self, data: str = None):
        super().__init__(-32050, data)


def make_message_with_result(result: str = None) -> Message:
    return Message(json.dumps({**MESSAGE_HEADER, "result": result}))


def make_request_message(method: str, params: list = []) -> Message:
    return Message(json.dumps({**MESSAGE_HEADER, "method": method, "params": params}))


def make_response_from_exception(ex: Exception) -> Message:
    if issubclass(type(ex), ValueError):
        ex = EthInvalidParams(str(ex))
    elif not issubclass(type(ex), EthException):
        ex = EthInternalError(str(ex))
    return Message(
        json.dumps(
            {
                **MESSAGE_HEADER,
                **ex.as_json(),
            }
        )
    )


def make_message_copy(msg: Message) -> Message:
    return Message(msg.as_raw_data())
