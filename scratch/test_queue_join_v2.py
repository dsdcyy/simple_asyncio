import os
import sys
import time

# 将当前目录添加到路径中以便导入 simple_asyncio
sys.path.insert(0, os.getcwd())

from simple_asyncio import run, sleep, AsyncQueue, create_task


async def producer(q, count):
    for i in range(count):
        await q.put(i)
        print(f"[Producer] Put item {i}")
        await sleep(0.05)


async def consumer(q, name):
    try:
        while True:
            item = await q.get()
            print(f"[Consumer {name}] Processing {item}...")
            await sleep(0.1)  # 模拟处理耗时
            print(f"[Consumer {name}] Finished {item}")
            q.task_done()
    except Exception:
        pass


async def test_join():
    q = AsyncQueue()
    num_items = 5

    # 启动任务
    create_task(producer(q, num_items))
    create_task(consumer(q, "A"))
    create_task(consumer(q, "B"))

    # 确保生产者至少运行了一步，关掉闸门
    await sleep(0.01)

    print("Main task waiting for join...")
    start_time = time.time()

    await q.join()

    end_time = time.time()
    print(f"Join finished in {end_time - start_time:.4f}s")
    print("SUCCESS: AsyncQueue.join() verified!")


if __name__ == "__main__":
    run(test_join())
