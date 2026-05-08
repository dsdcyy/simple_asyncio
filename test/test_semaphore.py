import simple_asyncio as asyncio
import time


async def limited_worker(semaphore, name):
    print(f"[{time.strftime('%H:%M:%S')}] 任务 {name} 正在排队...")
    async with semaphore:
        print(f"[{time.strftime('%H:%M:%S')}] 任务 {name} 抢到令牌！开始工作...")
        await asyncio.sleep(1)
        print(f"[{time.strftime('%H:%M:%S')}] 任务 {name} 完成并释放令牌。")


async def main():
    # 限制并发数为 2
    sem = asyncio.AsyncSemaphore(2)

    print("--- 开始测试 AsyncSemaphore (并发限制: 2) ---")

    # 启动 5 个并发任务
    await asyncio.gather(
        limited_worker(sem, "A"),
        limited_worker(sem, "B"),
        limited_worker(sem, "C"),
        limited_worker(sem, "D"),
        limited_worker(sem, "E"),
    )

    print("--- 测试完成 ---")


if __name__ == "__main__":
    asyncio.run(main())
