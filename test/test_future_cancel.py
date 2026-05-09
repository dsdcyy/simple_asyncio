#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
测试 Future cancel 功能
"""

from simple_asyncio import run, FutureCancelledError, get_event_loop


async def test_future_cancel():
    """测试 Future 的基本取消功能"""
    print("=" * 70)
    print("测试 1: Future 基本取消")
    print("=" * 70)

    loop = get_event_loop()

    # 创建一个 Future
    f = loop.create_future()

    print(f"\n初始状态:")
    print(f"  done: {f.done()}")
    print(f"  cancelled: {f.cancelled()}")

    # 取消 Future
    result = f.cancel(msg="测试取消")

    print(f"\n取消后:")
    print(f"  cancel 返回值: {result}")
    print(f"  done: {f.done()}")
    print(f"  cancelled: {f.cancelled()}")
    print(f"  cancel_msg: {f.cancel_msg()}")

    # 尝试再次取消
    result2 = f.cancel(msg="第二次取消")
    print(f"\n再次取消:")
    print(f"  cancel 返回值: {result2} (应该为 False)")

    # 验证异常
    try:
        f.result()
    except FutureCancelledError as e:
        print(f"\n✅ CancelledError raised: {e}")

    print("\n" + "=" * 70)
    print("测试 2: 已完成的 Future 不能被取消")
    print("=" * 70)

    f2 = loop.create_future()
    f2.set_result("已完成")

    result3 = f2.cancel(msg="尝试取消已完成的 Future")
    print(f"\n取消已完成的 Future:")
    print(f"  cancel 返回值: {result3} (应该为 False)")
    print(f"  cancelled: {f2.cancelled()} (应该为 False)")
    print(f"  result: {f2.result()}")

    print("\n" + "=" * 70)
    print("测试 3: Future 取消后设置结果会失败")
    print("=" * 70)

    f3 = loop.create_future()
    f3.cancel(msg="先取消")

    try:
        f3.set_result("尝试设置结果")
        print("❌ 不应该到达这里")
    except RuntimeError as e:
        print(f"✅ 正确抛出异常: {e}")


if __name__ == "__main__":
    run(test_future_cancel())
