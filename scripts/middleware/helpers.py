import asyncio
import logging

from functools import wraps
from contextlib import suppress

from logging import Logger


def log_exception(logger: Logger, ex: Exception, ctx: str = None):
    """
    Utility function for standard exception logging.

    Args:
        logger (Logger): The logger instance to use.
        ex (Exception): The exception to log.
        ctx (str, optional): Extra information to log with the exception.

    Usage:
        def my_func(args):
            try:
                # ...
            except Exception as ex:
                log_exception(logger, ex, "my_func")

    This function logs exceptions with customizable levels of detail based on the logger's level.
    It checks if the brief exception message and/or the full exception message should be logged
    based on the logger's level and the type of the exception.

    - If the logger's level is DEBUG, the function always logs the brief exception message.
    - If the logger's level is not DEBUG, the function only logs the brief exception message
      if the exception is not of type asyncio.CancelledError.

    - The function logs the full exception message if the logger's level is DEBUG or if
      the exception is of type AssertionError.
    """

    def should_log_brief_msg(logger: Logger, ex) -> bool:
        if logger.level <= logging.DEBUG:
            return True
        # Only log CancelledError on debug
        return not issubclass(type(ex), asyncio.CancelledError)

    def should_log_excetion(logger: Logger, ex) -> bool:
        if logger.level <= logging.DEBUG:
            return True
        # Always log these excetion types
        return issubclass(type(ex), AssertionError)

    if should_log_brief_msg(logger, ex):
        error_msg = f"{type(ex)} {ex}"
        if ctx:
            error_msg = f"{ctx=} {error_msg}"
        logger.error(error_msg)

    if should_log_excetion(logger, ex):
        logger.exception("Caught exception")


class log_on_exception:
    """
    Context class for logging the exception on exit.

    Usage:
        with log_on_exception(logger, ctx="my_context"):
            # Code that may raise an exception

    This context class is used to log any exception that occurs within the associated context block.
    It takes a logger instance and an optional context string as arguments. When an exception is raised
    within the context block, the `__exit__` method is called, and the exception is logged using the
    `log_exception` utility function.

    Args:
        logger (Logger): The logger instance to use.
        ctx (str, optional): Extra context information to log with the exception.
    """

    def __init__(self, logger: Logger, ctx: str = None):
        self.__logger = logger
        self.__ctx = ctx

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Log the exception on exit, if one occurred.
        """
        if exc_value:
            log_exception(self.__logger, exc_value, self.__ctx)


def log_on_exception_decorator(logger: Logger):
    """
    Decorator to log any exception raised from the function.

    Usage:
        @log_on_exception_decorator(logger)
        def my_function(args):
            ...

    This decorator is used to wrap a function and log any exception that may be raised during its execution.
    It takes a logger instance as an argument. When the decorated function is called, the decorator creates
    a wrapper function that includes a `with` statement using the `log_on_exception` context class. Any exception
    raised within the decorated function will be caught by the `with` statement and logged using the `log_exception`
    utility function.

    Args:
        logger (Logger): The logger instance to use for logging exceptions.

    Returns:
        Callable: The decorated function with exception logging behavior.

    """

    def inner(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with log_on_exception(logger, func.__name__):
                return func(*args, **kwargs)

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            with log_on_exception(logger, func.__name__):
                return await func(*args, **kwargs)

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return wrapper

    return inner


class log_and_suppress_exception(suppress):
    """
    Context class used to log an exception raised from a block, but prevent the exception from being propagated outside the block.

    This context class is a subclass of the built-in `suppress` context manager. It extends the functionality of `suppress`
    by logging any exception that may occur within the block using the provided logger instance, while still preventing
    the exception from being raised and propagated further.

    Args:
        logger (Logger): The logger instance to use for logging exceptions.
        ctx (str, optional): Extra context information to include in the log. Defaults to None.
        *exceptions: Variable-length list of exception types to suppress.

    Usage:
        with log_and_suppress_exception(logger, ctx, ExceptionType1, ExceptionType2):
            # Code block where exceptions are logged and suppressed

    In the above example, any exceptions of type ExceptionType1 or ExceptionType2 that occur within the block will be
    caught by the context manager. The exception will be logged using the provided logger along with any extra context
    information, and then the exception will be suppressed, preventing it from being raised outside the block.

    Note that this context class inherits from `suppress`, so it supports the same behavior as `suppress` for
    selectively suppressing specific exception types.

    """

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
    """
    Convenient context class that logs and suppresses exceptions of type Exception.

    This context class is a subclass of the `log_and_suppress_exception` context manager. It provides a convenient way to
    log and suppress exceptions of type `Exception` within a block of code.

    Args:
        logger (Logger): The logger instance to use for logging exceptions.
        ctx (str, optional): Extra context information to include in the log. Defaults to None.

    Usage:
        with log_and_suppress(logger, ctx):
            # Code block where exceptions of type Exception are logged and suppressed

    In the above example, any exceptions of type `Exception` that occur within the block will be caught by the context
    manager. The exception will be logged using the provided logger along with any extra context information, and then
    the exception will be suppressed, preventing it from being raised outside the block.

    Note that this context class inherits from `log_and_suppress_exception`, which in turn inherits from `suppress`.
    This means that it supports the same behavior as `log_and_suppress_exception` for selectively suppressing specific
    exception types.

    """

    def __init__(self, logger: Logger, ctx: str = None):
        super().__init__(logger, ctx, Exception)

    def __enter__(self):
        return super().__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        return super().__exit__(exc_type, exc_value, traceback)


def log_and_suppress_decorator(logger: Logger):
    """
    Decorator that applies the log_and_suppress context manager at the function level.

    This decorator function accepts a logger instance and returns a decorator that can be applied to functions. The
    decorator wraps the decorated function with the `log_and_suppress` context manager, which logs and suppresses
    exceptions of type Exception that occur within the function.

    Args:
        logger (Logger): The logger instance to use for logging exceptions.

    Returns:
        callable: A decorator that can be applied to functions.

    Usage:
        @log_and_suppress_decorator(logger)
        def my_func(args):
            # Function body

    In the above example, the `log_and_suppress_decorator` is used to decorate the `my_func` function. Any exceptions
    of type Exception that occur within `my_func` will be caught by the `log_and_suppress` context manager. The
    exceptions will be logged using the provided logger, and then they will be suppressed, preventing them from being
    raised outside the function.

    """

    def inner(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with log_and_suppress(logger, func.__name__):
                return func(*args, **kwargs)

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            with log_and_suppress(logger, func.__name__):
                return await func(*args, **kwargs)

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return wrapper

    return inner


def suppress_decorator(func):
    """
    Decorator that suppresses exceptions raised from the decorated function.

    The `suppress_decorator` is a decorator that wraps the given function with a context manager provided by the
    `contextlib.suppress` function. It allows the decorated function to execute without propagating any exceptions
    raised within it. Any exception raised will be caught and silently suppressed.

    Args:
        func: The function to be decorated.

    Returns:
        The decorated function.

    Usage:
        @suppress_decorator
        def my_function():
            ...

        @suppress_decorator
        async def my_async_function():
            ...

    Note:
        - The `suppress_decorator` can be used on regular synchronous functions as well as asynchronous coroutine
          functions.
        - Any exception raised from the decorated function will be caught and silently suppressed.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        with suppress(Exception):
            return func(*args, **kwargs)

    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        with suppress(Exception):
            return await func(*args, **kwargs)

    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    return wrapper


class printable_function:
    """
    A decorator that makes a function printable by overriding the __repr__ method.

    When applied to a function, this decorator allows the function object to be
    printed directly, resulting in the function name being displayed instead of
    the default representation ("<function func at 0x12345678>").

    Usage:
        @printable_function
        def my_func(args):
            pass

        print(my_func)  # Output: my_func
    """

    def __init__(self, func):
        self.func = func

    def __repr__(self):
        return self.func.__name__

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)


def entrypoint(func):
    """
    Decorator to mark a method as an entrypoint.

    Usage:
        @entrypoint
        def my_method(self, msg: Message) -> Message:
            # Method implementation

    Args:
        func: The method to be marked as an entrypoint.

    Returns:
        The decorated method.
    """
    func.__isentrypointmethod__ = True
    return func


def extract_entrypoints(clazz):
    """
    Generator function that traverses all attributes of a given class type to find
    the methods marked with the "entrypoint" decorator. It yields the name and value
    of each entrypoint method found.

    Args:
        clazz (type): The class type to extract entrypoints from.

    Yields:
        Tuple[str, Callable]: A tuple containing the name and value of each entrypoint method.

    Usage:
        class MyClass:
            @entrypoint
            def my_method(self, msg: Message) -> Message:
                # Entry point method implementation

        # Extract entrypoints from MyClass
        for name, method in extract_entrypoints(MyClass):
            # Process each entrypoint method

    Note:
        - The decorator "entrypoint" is used to mark methods as entrypoints.
        - The decorator should be defined as shown in the previous example.

    """
    for key, value in clazz.__dict__.items():
        if callable(value) and hasattr(value, "__isentrypointmethod__"):
            yield key, value
    for subclass in clazz.__bases__:
        yield from extract_entrypoints(subclass)


def unreachable_code():
    """
    This function only provides a more "verbose" way of marking unreachable code paths.
    """
    assert 0


def concat(parts: list, sep: str = ""):
    """
    Concatenate a list of strings using the given string separator.
    """
    return sep.join(parts)


def concat_bytes(parts: list, sep: bytes = b""):
    """
    Concatenate a list of bytes using the given byte separtor.
    """
    return sep.join(parts)


def get_or_default(config: dict, key: str, default):
    """
    Returns the value associated with the given key from the dictionary, or a default value if the key is not present.

    This function takes a dictionary `config`, a key `key`, and a default value `default`. It checks if the key exists in
    the dictionary. If the key is found, the corresponding value is returned. If the key is not found, the default value
    is returned instead.

    Args:
        config (dict): The dictionary to retrieve the value from.
        key (str): The key to look up in the dictionary.
        default: The default value to return if the key is not found.

    Returns:
        Any: The value associated with the key if it exists, or the default value if the key is not present.

    Usage:
        config = {'key1': 'value1', 'key2': 'value2'}
        result = get_or_default(config, 'key1', 'default_value')
        # If 'key1' exists in the config dictionary, result will be 'value1'
        # If 'key1' does not exist, result will be 'default_value'

    Note:
        - The `config` parameter must be of type dict.
        - The `key` parameter should be a string representing the key to look up.
        - The `default` parameter can be any type and serves as the fallback value.

    """
    assert isinstance(config, dict)
    if not key in config:
        return default
    assert isinstance(config[key], type(default))
    return config[key]
