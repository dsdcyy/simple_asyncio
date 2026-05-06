#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Author : Ljw
# @Time : 2026/5/6 17:11
# @FileName  :main_test.py
from simple_asyncio import *


def main():
    def task1():
        print("task1 开始")
        yield sleep(1)
        print("task1 睡醒")
        return "done1"

    def task2():
        print("task2 开始")
        yield sleep(0.5)
        print("task2 睡醒")
        return "done2"

    def cancel_task(task: Task):
        print("取消任务")
        yield sleep(0.6)
        r = task.cancel()
        if not r:
            print("任务未取消")

    loop = get_running_loop()
    task1_task = loop.create_task(task1())
    task2_task = loop.create_task(task2())  # 同时运行
    cancel_task_task = loop.create_task(cancel_task(task1_task))

    # print((yield task1_task))
    # print((yield task2_task))
    all_done_future = gather(task1_task, task2_task, cancel_task_task)
    print((yield all_done_future))
    print("全部结束")
    return None


def main2():
    def test_task(task_id, delay):
        # print(f"task{task_id} 开始")
        yield sleep(delay)
        # print(f"task{task_id} 睡醒")
        return f"done{task_id}"

    tasks = [test_task(i, 0.1) for i in range(10000)]
    all_done_future = gather(*tasks)
    # print((yield all_done_future))
    yield all_done_future
    print("全部结束")
    return None


def main3():
    def test_task(task_id, delay):
        # print(f"task{task_id} 开始")
        yield sleep(delay)
        # print(f"task{task_id} 睡醒")
        return f"done{task_id}"

    res = wait_for(test_task(0, 1), 1.5)
    print((yield res))
    return None


async def main4():
    s = AsyncSocket(socket.socket())
    await s.connect(("www.baidu.com", 80))
    await s.sendall(
        b"GET / HTTP/1.1\r\nHost: www.baidu.com\r\nConnection: close\r\n\r\n"
    )

    # 接收完整响应
    response = await s.recv_all()
    print(f"收到完整响应 ({len(response)} bytes)")

    # 分离头部和主体
    if b"\r\n\r\n" in response:
        headers, body = response.split(b"\r\n\r\n", 1)
        print("\n=== 响应头部 ===")
        print(headers.decode()[:500])
        print(f"\n=== 响应主体 ({len(body)} bytes) ===")
        print(body.decode()[:300])
    else:
        print(response.decode()[:500])

    s.close()


async def main5():
    """测试 Task name 和 cancel msg"""
    print("=" * 70)
    print("测试 Task name 和 cancel msg")
    print("=" * 70)

    loop = get_event_loop()

    # 创建带名称的任务
    async def long_task(name):
        print(f"[{name}] 开始执行...")
        try:
            await sleep(10)
            print(f"[{name}] 完成")
            return f"{name}_done"
        except CancelledError as e:
            print(f"[{name}] 被取消！")
            raise

    # 创建多个命名任务
    task1 = loop.create_task(long_task("Task-A"), name="HTTP-Request-1")
    task2 = loop.create_task(long_task("Task-B"), name="HTTP-Request-2")
    task3 = loop.create_task(long_task("Task-C"), name="Database-Query")

    print(f"\n任务列表:")
    print(f"  task1.name = {task1.name}")
    print(f"  task2.name = {task2.name}")
    print(f"  task3.name = {task3.name}")

    # 等待一会儿
    await sleep(0.5)

    # 取消 task2，带上原因
    print(f"\n取消 task2...")
    task2.cancel(msg="用户主动取消")

    # 再等一会儿
    await sleep(0.5)

    # 检查取消状态
    print(f"\ntask2 状态:")
    print(f"  cancelled: {task2.cancelled()}")
    print(f"  cancel_msg: {task2.cancel_msg()}")

    # 取消 task3，带上不同原因
    print(f"\n取消 task3...")
    task3.cancel(msg="超时限制")

    await sleep(0.5)

    print(f"\ntask3 状态:")
    print(f"  cancelled: {task3.cancelled()}")
    print(f"  cancel_msg: {task3.cancel_msg()}")

    # 清理 task1
    task1.cancel(msg="测试结束")

    print("\n✅ 测试完成")


async def main6():
    """测试 gather 失败时的 cancel msg"""
    print("\n" + "=" * 70)
    print("测试 gather 失败时的 cancel msg")
    print("=" * 70)

    async def success_task():
        print("[success] 开始")
        await sleep(0.5)
        print("[success] 完成")
        return "success"

    async def failing_task():
        print("[failing] 开始")
        await sleep(0.2)
        raise ValueError(" intentional error")

    try:
        results = await gather(success_task(), failing_task())
        print(f"结果: {results}")
    except ValueError as e:
        print(f"\n✅ Gather 失败，捕获异常: {e}")
        print("   其他任务应该已被取消并带有 cancel msg")


def main7():
    import asyncio

    async def asyncio_test():
        async def test_task(task_id, delay):
            await asyncio.sleep(delay)
            return f"done{task_id}"

        tasks = [test_task(i, 0.1) for i in range(10000)]
        results = await asyncio.gather(*tasks)

    asyncio.run(asyncio_test())


if __name__ == "__main__":
    start_time = time.time()
    run(main2())  # 框架测试
    print("总耗时：", time.time() - start_time)

    # run(main4())  # HTTP 请求测试
    # run(main5())  # Task name 和 cancel msg 测试
    # run(main6())  # gather cancel msg 测试
    start_time = time.time()
    main7()  # asyncio 测试
    print("总耗时：", time.time() - start_time)
