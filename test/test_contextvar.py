#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
测试 ContextVar 在事件循环中的优势
"""
import sys

sys.path.insert(0, "/media/Ljw/Data/fetch_tool")

from simple_asyncio import run, sleep, get_running_loop, get_event_loop


async def test_context_isolation():
    """测试上下文隔离 - 不同协程看到不同的事件循环"""
    print("=" * 70)
    print("测试 1: ContextVar 上下文隔离")
    print("=" * 70)

    loop1 = get_running_loop()
    print(f"\n[协程1] 事件循环 ID: {id(loop1)}")

    await sleep(0.1)

    loop2 = get_running_loop()
    print(f"[协程1] 睡眠后事件循环 ID: {id(loop2)}")
    print(f"相同循环: {loop1 is loop2}")

    assert loop1 is loop2, "应该是同一个事件循环"
    print("\n✅ 上下文隔离测试通过！")


async def test_nested_run_prevention():
    """测试嵌套 run() 被正确阻止"""
    print("\n" + "=" * 70)
    print("测试 2: 防止嵌套 run()")
    print("=" * 70)

    from simple_asyncio import _loop_var

    print(f"\n当前循环: {_loop_var.get()}")

    # 尝试嵌套调用 run()
    try:

        async def inner():
            return "inner"

        run(inner())  # 这应该抛出 RuntimeError
        print("❌ 应该抛出 RuntimeError")
    except RuntimeError as e:
        print(f"✅ 正确阻止嵌套: {e}")


def test_multiple_loops():
    """测试多个独立的事件循环"""
    print("\n" + "=" * 70)
    print("测试 3: 多个独立事件循环")
    print("=" * 70)

    from simple_asyncio import EventLoop, _loop_var

    # 第一个循环
    async def task1():
        loop = get_running_loop()
        print(f"\n[任务1] 事件循环 ID: {id(loop)}")
        await sleep(0.05)
        return "task1 done"

    result1 = run(task1())
    print(f"结果: {result1}")

    # 第二个循环（独立的）
    async def task2():
        loop = get_running_loop()
        print(f"[任务2] 事件循环 ID: {id(loop)}")
        await sleep(0.05)
        return "task2 done"

    result2 = run(task2())
    print(f"结果: {result2}")

    print("\n✅ 多个独立循环测试通过！")


async def test_context_inheritance():
    """测试上下文继承 - 子协程自动继承父协程的事件循环"""
    print("\n" + "=" * 70)
    print("测试 4: 上下文继承")
    print("=" * 70)

    parent_loop = get_running_loop()
    print(f"\n[父协程] 事件循环 ID: {id(parent_loop)}")

    async def child_coro():
        child_loop = get_running_loop()
        print(f"[子协程] 事件循环 ID: {id(child_loop)}")
        print(f"相同循环: {parent_loop is child_loop}")
        assert parent_loop is child_loop
        return "child done"

    result = await child_coro()
    print(f"结果: {result}")

    print("\n✅ 上下文继承测试通过！")


if __name__ == "__main__":
    print("开始 ContextVar 测试...\n")
    run(test_context_isolation())
    run(test_nested_run_prevention())
    test_multiple_loops()
    run(test_context_inheritance())
    print("\n" + "=" * 70)
    print("所有测试完成！🎉")
    print("=" * 70)
