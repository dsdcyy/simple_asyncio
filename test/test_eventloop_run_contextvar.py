#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
测试 EventLoop.run() 方法是否正确设置 ContextVar
"""

from simple_asyncio import EventLoop, get_running_loop


def test_eventloop_run_sets_contextvar():
    """测试 EventLoop.run() 方法是否正确设置 ContextVar"""
    print("=" * 70)
    print("测试: EventLoop.run() 设置 ContextVar")
    print("=" * 70)

    loop = EventLoop()

    async def check_loop():
        """在协程中检查是否能获取事件循环"""
        try:
            current_loop = get_running_loop()
            print(f"\n✅ 成功获取事件循环: {id(current_loop)}")
            print(f"   与创建的事件循环相同: {current_loop is loop}")
            assert current_loop is loop, "应该是同一个事件循环"
            return "success"
        except RuntimeError as e:
            print(f"\n❌ 无法获取事件循环: {e}")
            raise

    # 直接调用 EventLoop.run()
    result = loop.run(check_loop())
    print(f"\n任务结果: {result}")

    loop.close()
    print("\n✅ 测试通过！EventLoop.run() 正确设置了 ContextVar")


def test_without_contextvar_would_fail():
    """演示如果没有设置 ContextVar 会发生什么"""
    print("\n" + "=" * 70)
    print("演示: 没有 ContextVar 时的错误")
    print("=" * 70)

    from simple_asyncio import _loop_var

    # 手动清除 ContextVar
    token = _loop_var.set(None)

    try:
        loop = get_running_loop()
        print(f"❌ 不应该到达这里: {loop}")
    except RuntimeError as e:
        print(f"\n✅ 预期错误: {e}")
    finally:
        _loop_var.reset(token)


if __name__ == "__main__":
    test_eventloop_run_sets_contextvar()
    test_without_contextvar_would_fail()
