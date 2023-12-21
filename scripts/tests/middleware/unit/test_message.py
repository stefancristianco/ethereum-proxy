from middleware.message import EthException, EthParseError, EthInternalError

from middleware.message import eth_exception, raise_eth_exception
from middleware.helpers import unreachable_code


def test_eth_exception():
    @eth_exception(-1, "Test message")
    class TestError(EthException):
        pass

    try:
        TestError.create_and_raise("Some data")
        unreachable_code()
    except TestError as ex:
        assert ex.data == "Some data"
        assert ex.message == "Test message"
        assert ex.code == -1


def test_raise_eth_exception():
    try:
        raise_eth_exception(-32700)
        unreachable_code()
    except EthParseError as ex:
        assert ex.message == "Parse error"

    try:
        raise_eth_exception(-32603)
        unreachable_code()
    except EthInternalError as ex:
        assert ex.message == "Internal error"

    try:
        raise_eth_exception(10, "Some message", "Some data")
        unreachable_code()
    except EthException as ex:
        assert ex.code == 10
        assert ex.message == "Some message"
        assert ex.data == "Some data"
