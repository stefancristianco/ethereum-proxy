import logging

from logging import Logger
from utils.message import Message


def log_exception(logger: Logger, ex: Exception):
    logger.error(str(ex))
    if logger.level <= logging.DEBUG:
        logger.exception("Caught exception")


def get_or_default(config: dict, key: str, default):
    if not key in config:
        return default
    assert isinstance(config[key], type(default))
    return config[key]


def unreachable_code():
    assert 0


def is_response_success(msg: Message) -> bool:
    if len(msg) > 512:
        # Optimization: error messages are small in size
        return True
    msg_obj = msg.as_json()
    return "result" in msg_obj and msg_obj["result"]
