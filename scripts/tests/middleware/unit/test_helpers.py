import logging
import os
import pytest

from middleware.helpers import log_and_suppress_decorator

#
# Setup logger
#

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

logger.setLevel(os.environ.setdefault("LOG_LEVEL", "INFO"))


def test_log_and_suppress_decorator():
    @log_and_suppress_decorator(logger)
    def raise_exception():
        """Check that exception are hidden"""
        raise Exception()

    @log_and_suppress_decorator(logger)
    def pass_arguments(arg):
        """Check argument propagation"""
        return arg

    assert not raise_exception()
    assert pass_arguments(5) == 5


@pytest.mark.asyncio
async def test_async_log_and_suppress_decorator():
    @log_and_suppress_decorator(logger)
    async def raise_exception():
        """Check that exception are hidden"""
        raise Exception()

    @log_and_suppress_decorator(logger)
    async def pass_arguments(arg):
        """Check argument propagation"""
        return arg

    assert not await raise_exception()
    assert await pass_arguments(5) == 5
