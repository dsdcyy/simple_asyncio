#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
测试 Task 的 name 和 cancel 功能
"""

from simple_asyncio import run, sleep, FutureCancelledError, get_event_loop


async def test_task_name_and_cancel():
    """测试 Task 的命名和取消"""
    print("=" * 70)
    print("测试 1: Task 命名")
    print("=" * 70)

    loop = get_event_loop()

    async def worker(task_id):
        await sleep(0.1)
        return f"result_{task_id}"

    # 创建带名称的任务
    task1 = loop.create_task(worker(1), name="DataFetcher-1")
    task2 = loop.create_task(worker(2), name="DataFetcher-2")

    print(f"\n任务名称:")
    print(f"  task1.name: {task1._name}")
    print(f"  task2.name: {task2._name}")

    result1 = await task1
    result2 = await task2
    print(f"\n任务结果:")
    print(f"  {task1._name}: {result1}")
    print(f"  {task2._name}: {result2}")

    print("\n✅ 任务命名测试通过！")

    print("\n" + "=" * 70)
    print("测试 2: Task 取消")
    print("=" * 70)

    async def long_running_task():
        try:
            for i in range(10):
                print(f"  [Task] 执行第 {i+1} 步...")
                await sleep(0.5)
            return "完成"
        except FutureCancelledError as e:
            print(f"  [Task] 被取消: {e}")
            raise

    task3 = loop.create_task(long_running_task(), name="LongRunningTask")

    # 等待 1.5 秒后取消
    await sleep(1.5)
    print(f"\n[主协程] 取消任务 {task3._name}...")
    result = task3.cancel(msg="用户主动取消")
    print(f"[主协程] cancel 返回值: {result}")

    await sleep(0.2)

    print(f"\n[主协程] Task 状态:")
    print(f"  done: {task3.done()}")
    print(f"  cancelled: {task3.cancelled()}")
    print(f"  cancel_msg: {task3.cancel_msg()}")

    try:
        await task3
        print("❌ 不应该到达这里")
    except FutureCancelledError as e:
        print(f"\n✅ 正确抛出 CancelledError: {e}")

    print("\n" + "=" * 70)
    print("测试 3: 已完成的任务不能被取消")
    print("=" * 70)

    async def quick_task():
        await sleep(0.1)
        return "快速完成"

    task4 = loop.create_task(quick_task(), name="QuickTask")
    await task4

    result2 = task4.cancel(msg="尝试取消已完成的任务")
    print(f"\n取消已完成的任务:")
    print(f"  cancel 返回值: {result2} (应该为 False)")
    print(f"  cancelled: {task4.cancelled()} (应该为 False)")
    print(f"  result: {task4.result()}")

    print("\n✅ 所有测试通过！🎉")


if __name__ == "__main__":
    run(test_task_name_and_cancel())
