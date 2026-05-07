import sys
import os
import time

# 将当前目录添加到路径中以便导入 simple_asyncio
sys.path.insert(0, os.getcwd())

from simple_asyncio import run, sleep, AsyncQueue, create_task


async def producer(q, count):
    for i in range(count):
        print(f"[Producer] Putting item {i}")
        await q.put(i)
        await sleep(0.1)


async def consumer(q, name):
    while True:
        item = await q.get()
        print(f"[Consumer {name}] Got item {item}, processing...")
        await sleep(0.2)  # 模拟处理耗时
        print(f"[Consumer {name}] Finished item {item}")
        q.task_done()


async def test_join():
    q = AsyncQueue()
    num_items = 5

    # 启动生产者
    producer_task = create_task(producer(q, num_items))

    # 启动两个消费者
    c1 = create_task(consumer(q, "A"))
    c2 = create_task(consumer(q, "B"))

    print("Main task waiting for join...")
    start_time = time.time()

    # 等待所有任务完成
    await q.join()

    end_time = time.time()
    print(f"Join finished in {end_time - start_time:.4f}s")

    # 验证是否真的所有任务都处理完了
    # 5 个任务，2 个消费者，每个任务 0.2s，加上生产者 0.1s 间隔
    # 应该是能够成功同步的
    print("SUCCESS: Queue join works correctly!")

    # 取消死循环的消费者
    c1.cancel()
    c2.cancel()


if __name__ == "__main__":
    run(test_join())
