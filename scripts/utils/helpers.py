import logging

from logging import Logger


def log_exception(logger: Logger, ex: Exception):
    logger.error(str(ex))
    if logger.level <= logging.DEBUG:
        logger.exception("Caught exception")


def get_or_default(config: dict, key: str, default):
    if not key in config:
        return default
    return config[key]


def unreachable_code():
    assert 0
