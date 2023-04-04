from extensions.extension_base import Extension, ExtensionBase
from utils.message import Message


class RoundRobinSelector(Extension):
    def __init__(self, name):
        super().__init__(name)

        self.__next_handlers = []
        self.__rr_counter = 0

    async def _handle_request(self, request: Message) -> Message:
        assert len(self.__next_handlers)

        self.__rr_counter += 1
        return await self.__next_handlers[
            self.__rr_counter % len(self.__next_handlers)
        ].do_handle_request(request)

    def _add_next_handler(self, next_handler: ExtensionBase):
        self.__next_handlers.append(next_handler)

    async def _initialize(self):
        for handler in self.__next_handlers:
            await handler.do_initialize()

    async def _cancel(self):
        for handler in self.__next_handlers:
            await handler.do_cancel()
