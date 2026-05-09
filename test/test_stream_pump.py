import simple_asyncio as asyncio
import time


async def producer(queue):
    for i in range(5):
        await asyncio.sleep(0.1)
        print(f"[Producer] 生产数据: {i}")
        await queue.put(i)
    print("[Producer] 生产任务完成")


async def main():
    queue = asyncio.AsyncQueue()

    print("--- 开始测试 stream_from_queue_sentinel ---")

    # 1. 启动生产者任务
    p_task = asyncio.create_task(producer(queue), name="ProducerTask")

    # 2. 使用流式泵消费队列
    # 它会自动监听 p_task，当 p_task 完成时，泵会通过哨兵机制安全退出
    count = 0
    start_time = time.monotonic()

    print("[Consumer] 启动流式泵迭代...")
    async for item in asyncio.stream_from_queue_sentinel(queue, p_task):
        print(f"[Consumer] 收到流数据: {item}")
        count += 1

    end_time = time.monotonic()
    print(f"--- 测试完成 ---")
    print(f"总计接收数据: {count} 条 (预期: 5)")
    print(f"迭代耗时: {end_time - start_time:.2f}s")

    # 验证队列是否已清空（包括 task_done 的计数）
    await queue.join()
    print("队列 join 成功，所有任务计数已清零")


if __name__ == "__main__":
    asyncio.run(main())
