import sys
import os

# 将当前目录添加到路径中以便导入 simple_asyncio
sys.path.insert(0, os.getcwd())

from simple_asyncio import run, sleep, AsyncQueue, create_task


async def test_queue_fix():
    q = AsyncQueue(maxsize=1)

    # 1. 验证 _wake_getter 循环逻辑
    get_task_cancelled = create_task(q.get())
    get_task_valid = create_task(q.get())
    await sleep(0.01)

    get_task_cancelled.cancel()
    await sleep(0.01)

    # put 应该跳过被取消的 get_task_cancelled，直接唤醒 get_task_valid
    await q.put("item1")
    res = await get_task_valid
    print(f"Got item: {res}")
    if res == "item1":
        print("SUCCESS: _wake_getter loop verified!")

    # 2. 验证 _wake_putter 循环逻辑 (丢数据测试)
    # 队列满
    await q.put("item2")

    # 让两个 put 等待
    put_task_cancelled = create_task(q.put("item3-cancelled"))
    put_task_valid = create_task(q.put("item3-valid"))
    await sleep(0.01)

    put_task_cancelled.cancel()
    await sleep(0.01)

    # get 一个，应该触发 _wake_putter。它应该跳过 cancelled，把 valid 放入
    val2 = await q.get()
    print(f"Got item from q: {val2}")  # item2

    await sleep(0.01)
    print(f"Queue size (should be 1 if item3-valid was put): {q.qsize()}")
    if q.qsize() == 1:
        val3 = await q.get()
        print(f"Got last item: {val3}")
        if val3 == "item3-valid":
            print("SUCCESS: _wake_putter loop and no data loss verified!")


if __name__ == "__main__":
    run(test_queue_fix())
