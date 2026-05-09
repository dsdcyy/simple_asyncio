import os
import sys

# 将当前目录添加到路径中以便导入 simple_asyncio
sys.path.insert(0, os.getcwd())

from simple_asyncio import run, Event


async def test_event_clear():
    ev = Event()
    print("Setting event...")
    ev.set()
    print(f"Is set: {ev.is_set()}")

    print("Clearing event...")
    ev.clear()
    print(f"Is set: {ev.is_set()}")

    print("Waiting on cleared event (should block)...")
    try:
        # 这个 wait 应该阻塞，但由于 bug，它会立即返回
        import time

        start = time.time()
        # 给一个超短的超时，看它是否立即返回
        # 注意我们的 wait 不带超时，所以我们用 loop.call_later 来模拟超时或者观察行为
        # 这里我们直接运行，看看它是否耗时
        await ev.wait()
        end = time.time()
        print(f"Wait returned in {end - start:.4f}s")
        if end - start < 0.1:
            print("BUG CONFIRMED: Wait returned immediately on a cleared event!")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    run(test_event_clear())
