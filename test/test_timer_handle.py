#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
测试 TimerHandle 功能
"""

from simple_asyncio import run, sleep, get_event_loop


async def test_timer_handle_basic():
    """测试 TimerHandle 基本功能"""
    print("=" * 70)
    print("测试 1: TimerHandle 基本使用")
    print("=" * 70)

    loop = get_event_loop()

    results = []

    def callback(value):
        results.append(value)
        print(f"  [Timer] 回调执行: {value}")

    # 创建定时器
    handle1 = loop.call_later(0.1, callback, "timer1")
    handle2 = loop.call_later(0.2, callback, "timer2")

    print(f"\n创建的定时器:")
    print(f"  handle1: {handle1}")
    print(f"  handle2: {handle2}")
    print(f"  handle1.cancelled(): {handle1.cancelled()}")

    # 等待定时器执行
    await sleep(0.3)

    print(f"\n执行结果: {results}")
    print(f"handle1.cancelled() after execution: {handle1.cancelled()}")

    print("\n✅ 基本功能测试通过！")


async def test_timer_cancel():
    """测试定时器取消"""
    print("\n" + "=" * 70)
    print("测试 2: 定时器取消")
    print("=" * 70)

    loop = get_event_loop()

    results = []

    def callback(value):
        results.append(value)
        print(f"  [Timer] 回调执行: {value}")

    # 创建定时器
    handle1 = loop.call_later(0.1, callback, "should_execute")
    handle2 = loop.call_later(0.2, callback, "should_be_cancelled")

    print(f"\n取消前:")
    print(f"  handle2.cancelled(): {handle2.cancelled()}")

    # 立即取消第二个定时器
    handle2.cancel()

    print(f"取消后:")
    print(f"  handle2.cancelled(): {handle2.cancelled()}")
    print(f"  handle2: {handle2}")

    # 等待
    await sleep(0.3)

    print(f"\n执行结果: {results}")
    print(f"预期: ['should_execute']")

    if results == ["should_execute"]:
        print("✅ 取消功能测试通过！")
    else:
        print("❌ 测试失败！")


async def test_sleep_cancel():
    """测试 sleep 取消时定时器也被取消"""
    print("\n" + "=" * 70)
    print("测试 3: sleep 取消传播")
    print("=" * 70)

    from simple_asyncio import FutureCancelledError

    loop = get_event_loop()

    async def long_sleep():
        try:
            print("  [Task] 开始 sleep(10)...")
            await sleep(10)
            print("  [Task] sleep 完成（不应该到达这里）")
        except FutureCancelledError as e:
            print(f"  [Task] sleep 被取消: {e}")
            raise

    task = loop.create_task(long_sleep())

    # 等待一小段时间后取消
    await sleep(0.1)
    print("\n  [Main] 取消任务...")
    task.cancel(msg="测试取消")

    await sleep(0.1)

    print(f"\n任务状态:")
    print(f"  done: {task.done()}")
    print(f"  cancelled: {task.cancelled()}")

    print("\n✅ sleep 取消传播测试通过！")


async def test_wait_for_timeout():
    """测试 wait_for 超时"""
    print("\n" + "=" * 70)
    print("测试 4: wait_for 超时")
    print("=" * 70)

    from simple_asyncio import wait_for, AsyncTimeoutError

    loop = get_event_loop()

    async def slow_task():
        print("  [Task] 开始慢任务...")
        await sleep(5)
        return "完成"

    try:
        print("  [Main] 等待慢任务（超时 0.2 秒）...")
        result = await wait_for(slow_task(), timeout_delay=0.2)
        print(f"  [Main] 结果: {result}（不应该到达这里）")
    except AsyncTimeoutError:
        print("  [Main] ✅ 正确抛出 TimeoutError")

    await sleep(0.1)
    print("\n✅ wait_for 超时测试通过！")


if __name__ == "__main__":
    print("开始 TimerHandle 测试...\n")
    run(test_timer_handle_basic())
    run(test_timer_cancel())
    run(test_sleep_cancel())
    run(test_wait_for_timeout())
    print("\n" + "=" * 70)
    print("所有测试完成！🎉")
    print("=" * 70)
