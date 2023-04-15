from extensions.abstract.extension_base import Extension, ExtensionBase
from utils.message import Message


class RoundRobinSelector(Extension):
    def __init__(self, name: str = "RoundRobinSelector"):
        super().__init__(name)

        self.__next_handlers = []
        self.__rr_counter = 0

    def __repr__(self) -> str:
        return "RoundRobinSelector()"

    async def _handle_request(self, request: Message) -> Message:
        assert len(self.__next_handlers)

        self.__rr_counter += 1
        return await self.__next_handlers[
            self.__rr_counter % len(self.__next_handlers)
        ].do_handle_request(request)

    def _add_next_handler(self, next_handler: ExtensionBase):
        self.__next_handlers.append(next_handler)
