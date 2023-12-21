import asyncio
import logging
import os

from middleware.logging_helpers import with_log_on_exception

#
# Setup logger
#

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

logger.setLevel(os.environ.setdefault("LOG_LEVEL", "INFO"))


class ElasticTaskPool:
    class IdleTimeout(Exception):
        pass

    class ContextQueueReader:
        def __init__(self, task_pool: "ElasticTaskPool", worker_id: int):
            self.__id = worker_id
            self.__task_pool = task_pool

        def __enter__(self):
            return self.__queue_reader()

        def __exit__(self, exc_type, exc_value, traceback):
            self.__task_pool.signal_stopping(self.__id)
            # Silently ignore IdleTimeout exceptions
            return not exc_value or issubclass(exc_type, ElasticTaskPool.IdleTimeout)

        async def __queue_reader(self):
            while True:
                try:
                    print(f"queue_reader start read {self.__task_pool.queue.qsize()}")
                    item = await self.__task_pool.queue.get()
                    # item = await asyncio.wait_for(
                    #     self.__task_pool.queue.get(), self.__task_pool.idle_timeout
                    # )
                except asyncio.TimeoutError:
                    # Special error handling when 'idle_timeout' expires
                    raise ElasticTaskPool.IdleTimeout()

                self.__task_pool.signal_busy()
                try:
                    print("queue_reader yield item")
                    yield item
                finally:
                    self.__task_pool.queue.task_done()
                    self.__task_pool.signal_available()

    def __init__(self, max_workers: int, max_queue_size: int, idle_timeout: int):
        self.__queue = asyncio.Queue(max_queue_size)

        self.__max_workers = max_workers
        self.__idle_timeout = idle_timeout

        self.__total_workers = 0
        self.__available_workers = 0
        self.__uuid = 0
        self.__workers_table = {}

        self.__pool_started = asyncio.Event()

    @property
    def queue(self):
        return self.__queue

    @property
    def idle_timeout(self):
        return self.__idle_timeout

    def set_worker_logic(self, worker_logic):
        self.__worker_logic = worker_logic

    async def put(self, request):
        """ """

        # Wait for the pool to start
        await self.__pool_started.wait()
        print("elastic_task_pool started")
        # If no worker is imediatelly available to process the request,
        # we may create one, up to the given max workers.
        if not self.__available_workers and self.__total_workers < self.__max_workers:
            worker_id = self.__uuid
            self.__uuid += 1
            print("elastic_task_pool creating worker")

            @with_log_on_exception(logger)
            async def worker_logic_wrapper():
                print(1)
                with ElasticTaskPool.ContextQueueReader(worker_id) as queue_reader:
                    print(2)
                    await self.__worker_logic(queue_reader)
                    print(3)

            self.__workers_table[worker_id] = asyncio.create_task(
                worker_logic_wrapper()
            )
            self.__available_workers += 1
            self.__total_workers += 1
            print(
                f"elastic_task_pool {self.__available_workers=} {self.__total_workers=}"
            )

        print(f"adding to queue {request}")
        await self.__queue.put(request)

    async def __aenter__(self):
        assert self.__worker_logic
        self.__pool_started.set()

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.__pool_started.clear()

        tasks = list(self.__workers_table.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        assert not self.__available_workers
        assert not self.__total_workers

    def __len__(self):
        return self.__queue.qsize()

    def signal_busy(self):
        assert self.__available_workers > 0
        self.__available_workers -= 1

    def signal_available(self):
        assert self.__available_workers < self.__total_workers
        self.__available_workers += 1

    def signal_stopping(self, id: int):
        assert self.__total_workers > 0
        assert id in self.__workers_table

        self.__total_workers -= 1
        self.__available_workers -= 1
        del self.__workers_table[id]
