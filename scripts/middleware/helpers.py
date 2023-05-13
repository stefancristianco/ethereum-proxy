import logging

from contextlib import suppress

from logging import Logger


def log_exception(logger: Logger, ex: Exception, ctx: str = None):
    def should_log_excetion(logger: Logger, ex) -> bool:
        if logger.level <= logging.DEBUG:
            return True
        # Always log these excetion types
        return issubclass(type(ex), AssertionError)

    if ctx:
        logger.error(f"{ctx=}")
    logger.error(f"{type(ex)} {ex}")
    if should_log_excetion(logger, ex):
        logger.exception("Caught exception")


class log_on_exception:
    def __init__(self, logger: Logger, ctx: str = None):
        self.__logger = logger
        self.__ctx = ctx

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_value:
            log_exception(self.__logger, exc_value, self.__ctx)


class log_and_suppress_exception(suppress):
    def __init__(self, logger: Logger, ctx: str = None, *exceptions):
        super().__init__(exceptions)

        self.__logger = logger
        self.__ctx = ctx

    def __enter__(self):
        return super().__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_value:
            log_exception(self.__logger, exc_value, self.__ctx)
        return super().__exit__(exc_type, exc_value, traceback)


class log_and_suppress(log_and_suppress_exception):
    def __init__(self, logger: Logger, ctx: str = None):
        super().__init__(logger, ctx, Exception)

    def __enter__(self):
        return super().__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        return super().__exit__(exc_type, exc_value, traceback)


def unreachable_code():
    assert 0


def concat(parts: list, sep: str = ""):
    return sep.join(parts)


def concat_bytes(parts: list, sep: bytes = b""):
    return sep.join(parts)


def get_or_default(config: dict, key: str, default):
    assert isinstance(config, dict)
    if not key in config:
        return default
    assert isinstance(config[key], type(default))
    return config[key]
