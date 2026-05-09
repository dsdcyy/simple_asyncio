import time

import simple_asyncio as asyncio


async def worker(lock, name, delay):
    # 使用上下文管理器自动获取和释放 ID
    async with lock as tid:
        print(
            f"[{time.strftime('%H:%M:%S')}] 任务 {name} 启动, 领到 ID: {tid}, 预计耗时 {delay}s"
        )
        await asyncio.sleep(delay)
        print(f"[{time.strftime('%H:%M:%S')}] 任务 {name} (ID: {tid}) 完成")


async def main():
    lock = asyncio.AsyncSelectiveLock()

    print("--- 开始测试 AsyncSelectiveLock ---")

    # 1. 启动一系列不同耗时的任务
    # ID 将依次为 1, 2, 3, 4
    asyncio.gather(
        worker(lock, "快速-1", 1),
        worker(lock, "慢速-2", 4),
        worker(lock, "快速-3", 2),
        worker(lock, "中速-4", 3),
    )

    # 稍微等一下，确保任务都拿到了 ID 并开始运行
    await asyncio.sleep(0.1)

    # 2. 局部等待测试：只等 ID 1 和 3
    print(f"[{time.strftime('%H:%M:%S')}] 【观察者 A】开始等待 ID { {1, 3} }...")
    start_a = time.monotonic()
    await lock.wait_unlock(target_ids={1, 3})
    end_a = time.monotonic()
    print(
        f"[{time.strftime('%H:%M:%S')}] 【观察者 A】醒了！耗时 {end_a - start_a:.2f}s (预期约 2s)"
    )

    # 3. 全局等待测试：等所有人
    print(f"[{time.strftime('%H:%M:%S')}] 【观察者 B】开始等待所有任务...")
    # start_b = time.monotonic()
    await lock.wait_unlock()  # 不传 ID 即为全局等待
    end_b = time.monotonic()
    print(
        f"[{time.strftime('%H:%M:%S')}] 【观察者 B】醒了！耗时 {end_b - start_a:.2f}s (预期约 4s)"
    )

    print("--- 测试完成 ---")


if __name__ == "__main__":
    asyncio.run(main())
