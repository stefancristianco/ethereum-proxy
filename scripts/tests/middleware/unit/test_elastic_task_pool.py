import asyncio
import pytest

from middleware.helpers import wait_response, request_resolver

from middleware.elastic_task_pool import ElasticTaskPool


@pytest.mark.asyncio
async def test_basic_operations():
    task_pool = ElasticTaskPool(10, 10, 1)

    async def use_task_pool():
        for nr in range(1, 10):
            print(f"use_task_pool: {nr=}")
            async with await wait_response(task_pool, nr) as response:
                print(f"use_task_pool {response=}")
                assert response == nr + 1

    async def worker_logic(queue_reader):
        print(f"worker_logic called")
        async for pending_response in queue_reader:
            print(f"worker_logic got response")
            with request_resolver(pending_response) as request:
                print(f"worker_logic got {request=}")
                pending_response.set_result(request + 1)

    task_pool.set_worker_logic(worker_logic)
    async with task_pool:
        tasks = [use_task_pool() for _ in range(0, 10)]
        await asyncio.wait_for(asyncio.gather(*tasks), 10)
