import logging

from logging import Logger


def log_exception(logger: Logger, ex: Exception):
    logger.error(str(ex))
    if logger.level <= logging.DEBUG:
        logger.exception("Caught exception")
