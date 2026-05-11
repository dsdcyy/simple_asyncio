import sys
import os
import time

# 确保能找到 simple_asyncio
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import random

from simple_asyncio import AsyncSelectiveLock, sleep, run, get_running_loop


async def worker(lock: AsyncSelectiveLock, worker_id: int):
    # print(f"任务 {worker_id} 启动...")
    duration = random.uniform(0.1, 1)
    await sleep(duration)
    print(f"任务 {worker_id} 释放 (耗时 {duration:.2f}s 为模拟延迟)")


async def test_wait_count():
    lock = AsyncSelectiveLock()
    loop = get_running_loop()

    # 1. 启动 10 个任务，耗时为0.1s到1s之间的随机数
    for i in range(1, 11):
        t = loop.create_task(worker(lock, i))
        lock.trace_task(t)

    # 给一点点时间让所有 worker 跑起来并领到锁
    await sleep(0.1)

    print(f"\n--- 开始等待阈值：只要 10 个任务中有 5 个完成即可 ---")

    start_time = time.monotonic()

    # 2. 等待其中任意 5 个完成
    target_ids = set(range(1, 11))
    unlocked_ids = await lock.wait_count(target_ids, count=5)

    end_time = time.monotonic()

    print(f"\n✅ 阈值达成！")
    print(f"耗时: {end_time - start_time:.2f}s")
    print(f"此时已解锁的 ID: {unlocked_ids}")
    print(f"剩余锁定的 ID 数量: {len(lock.locked_ids(target_ids))}")
    await lock.wait_unlock()
    end_time = time.monotonic()
    print(f"所有任务均完成，耗时: {end_time - start_time:.2f}s")


if __name__ == "__main__":
    run(test_wait_count())
