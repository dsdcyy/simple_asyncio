import simple_asyncio as asyncio


async def buggy_producer(queue):
    await queue.put("Good Data")
    await asyncio.sleep(0.1)
    print("[Producer] 准备抛出异常...")
    raise RuntimeError("生产者崩溃了！")


async def main():
    queue: asyncio.AsyncQueue[str] = asyncio.AsyncQueue()
    print("--- 开始测试流式泵异常传播 ---")

    p_task = asyncio.create_task(buggy_producer(queue))

    try:
        async for item in asyncio.stream_from_queue_sentinel(queue, p_task):
            print(f"[Consumer] 收到: {item}")
    except RuntimeError as e:
        print(f"[Consumer] 成功捕获到来自生产者的异常: {e}")

    print("--- 异常传播测试完成 ---")


if __name__ == "__main__":
    asyncio.run(main())
