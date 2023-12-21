import json
import copy

from middleware.helpers import unreachable_code

MESSAGE_HEADER = {"jsonrpc": "2.0", "id": 0}
RESTRICTED_KEYS = {"__data__", "__json__", "__hash__", "__hash_data__"}


class Message:
    def __init__(self, data):
        if isinstance(data, bytes):
            self.__attachments = {"__data__": data}
        elif isinstance(data, str):
            self.__attachments = {"__data__": data.encode()}
        elif isinstance(data, dict):
            self.__attachments = {"__json__": data}
        else:
            unreachable_code()

    def __repr__(self) -> str:
        return f"Message({self.as_raw_data()})"

    def __getitem__(self, key: str):
        assert key not in RESTRICTED_KEYS
        return self.__attachments[key]

    def __setitem__(self, key: str, value):
        assert key not in RESTRICTED_KEYS
        self.__attachments[key] = value

    def __delitem__(self, key: str):
        assert key not in RESTRICTED_KEYS
        del self.__attachments[key]

    def __contains__(self, key: str):
        return key in self.__attachments

    def __len__(self):
        return len(self.as_raw_data())

    def __hash__(self) -> int:
        """
        Produce a hash from the message body.
        The variable part is removed firs (e.g. {'id': xxx}).
        """
        if not "__hash__" in self.__attachments:
            adjusted_message = dict(self.as_json())
            del adjusted_message["id"]
            del adjusted_message["jsonrpc"]
            self.__attachments["__hash_data__"] = json.dumps(adjusted_message)
            self.__attachments["__hash__"] = hash(self.__attachments["__hash_data__"])
        return self.__attachments["__hash__"]

    def __eq__(self, value: object) -> bool:
        return (
            isinstance(value, Message)
            and hash(self) == hash(value)
            and (
                self.__attachments["__hash_data__"]
                == value.__attachments["__hash_data__"]
            )
        )

    def as_json(self, key: str = None):
        if not "__json__" in self.__attachments:
            self.__attachments["__json__"] = json.loads(self.__attachments["__data__"])
        if key:
            return self.__attachments["__json__"][key]
        return self.__attachments["__json__"]

    def as_json_copy(self):
        return copy.deepcopy(self.as_json())

    def as_raw_data(self):
        if not "__data__" in self.__attachments:
            self.__attachments["__data__"] = json.dumps(
                self.__attachments["__json__"]
            ).encode()
        return self.__attachments["__data__"]


class EthException(Exception):
    def __init__(self, code: int, message: str, data: str = None):
        self.code = code
        self.message = message
        self.data = data

    def as_json(self):
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "data": self.data,
            }
        }


def eth_exception(code: int, message: str):
    def inner(clazz):
        assert issubclass(clazz, EthException)

        def init(self, data: str = None):
            EthException.__init__(self, code, message, data)

        setattr(clazz, "__init__", init)

        @classmethod
        def create_and_raise(clazz, data: str = None):
            raise clazz(data)

        setattr(clazz, "create_and_raise", create_and_raise)

        @classmethod
        def register_type(clazz, error_table: dict):
            assert code not in error_table
            error_table[code] = clazz.create_and_raise

        setattr(clazz, "register_type", register_type)

        return clazz

    return inner


@eth_exception(-32700, "Parse error")
class EthParseError(EthException):
    """Invalid JSON"""

    pass


@eth_exception(-32600, "Invalid request")
class EthInvalidRequest(EthException):
    """JSON is not a valid request object"""

    pass


@eth_exception(-32601, "Method not found")
class EthMethodNotFound(EthException):
    """Method does not exist"""

    pass


@eth_exception(-32602, "Invalid params")
class EthInvalidParams(EthException):
    """Invalid method parameters"""

    pass


@eth_exception(-32603, "Internal error")
class EthInternalError(EthException):
    """Internal JSON-RPC error"""

    pass


@eth_exception(-32004, "Method not supported")
class EthMethodNotSupported(EthException):
    """Method is not implemented"""

    pass


@eth_exception(-32005, "Limit exceeded")
class EthLimitExceeded(EthException):
    """Request exceeds defined limit"""

    pass


@eth_exception(-32006, "JSON-RPC version not supported")
class EthJsonVersion(EthException):
    """Version of JSON-RPC protocol is not supported"""

    pass


@eth_exception(-32050, "Not supported")
class EthNotSupported(EthException):
    """Request not supported by proxy"""

    pass


@eth_exception(-32098, "Capacity exceeded")
class EthCapacityExceeded(EthException):
    """Capacity exceeded custom error code"""

    pass


def make_message_with_result(result=None) -> Message:
    return Message({**MESSAGE_HEADER, "result": result})


def make_request_message(method: str, params: list = []) -> Message:
    return Message({**MESSAGE_HEADER, "method": method, "params": params})


def make_response_from_exception(ex: Exception) -> Message:
    if issubclass(type(ex), ValueError):
        ex = EthInvalidParams(str(ex))
    elif not issubclass(type(ex), EthException):
        ex = EthInternalError(str(ex))
    return Message(
        {
            **MESSAGE_HEADER,
            **ex.as_json(),
        }
    )


def raise_eth_exception(code: int, message: str = None, data: str = None):
    class ErrorTable:
        __error_table = {}

        @classmethod
        @property
        def error_table(clazz):
            if not clazz.__error_table:
                EthParseError.register_type(clazz.__error_table)
                EthInvalidRequest.register_type(clazz.__error_table)
                EthMethodNotFound.register_type(clazz.__error_table)
                EthInvalidParams.register_type(clazz.__error_table)
                EthInternalError.register_type(clazz.__error_table)
                EthMethodNotSupported.register_type(clazz.__error_table)
                EthLimitExceeded.register_type(clazz.__error_table)
                EthJsonVersion.register_type(clazz.__error_table)
                EthNotSupported.register_type(clazz.__error_table)

            return clazz.__error_table

    if code in ErrorTable.error_table:
        ErrorTable.error_table[code](data)

    raise EthException(code, message, data)


def check_for_error_message(msg: Message) -> Message:
    if len(msg) > 512:
        # Optimization: error messages are small in size
        return msg

    msg_obj = msg.as_json()
    if "error" in msg_obj:
        code = msg_obj["error"]["code"] if "code" in msg_obj["error"] else -32603
        message = msg_obj["error"]["message"] if "message" in msg_obj["error"] else None
        data = msg_obj["error"]["data"] if "data" in msg_obj["error"] else None
        raise_eth_exception(code, message, data)
    return msg


def make_message_copy(msg: Message) -> Message:
    return Message(msg.as_raw_data())


def is_response_success(msg: Message) -> bool:
    if len(msg) > 512:
        # Optimization: error messages are small in size
        return True
    msg_obj = msg.as_json()
    return "result" in msg_obj and msg_obj["result"]


def set_no_cache_tag(msg: Message) -> Message:
    msg["__no_cache__"] = True
    return msg


def has_no_cache_tag(msg: Message) -> bool:
    return "__no_cache__" in msg
