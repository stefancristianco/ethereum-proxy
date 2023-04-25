import logging

from logging import Logger


class WithBase:
    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        pass


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


class log_on_exception(WithBase):
    def __init__(self, logger: Logger, ctx: str = None):
        self.__logger = logger
        self.__ctx = ctx

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_value:
            log_exception(self.__logger, exc_value, self.__ctx)


class log_and_suppress_exception(WithBase):
    def __init__(self, logger: Logger, *exceptions, ctx: str = None):
        self.__logger = logger
        self.__ctx = ctx
        self.__exceptions = exceptions

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_value:
            log_exception(self.__logger, exc_value, self.__ctx)
        return exc_type and issubclass(exc_type, self.__exceptions)


class log_and_suppress(log_and_suppress_exception):
    def __init__(self, logger: Logger, ctx: str = None):
        super().__init__(logger, Exception, ctx=ctx)

    def __enter__(self):
        return super().__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        return super().__exit__(exc_type, exc_value, traceback)


def get_or_default(config: dict, key: str, default):
    if not key in config:
        return default
    assert isinstance(config, dict)
    assert isinstance(config[key], type(default))
    return config[key]


def unreachable_code():
    assert 0
