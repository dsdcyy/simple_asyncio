import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import simple_asyncio as asyncio


async def task_ok():
    await asyncio.sleep(0.1)
    return "ok"


async def task_error():
    await asyncio.sleep(0.2)
    raise ValueError("BOM!")


async def main():
    print("--- 测试 return_exceptions=False (默认) ---")
    try:
        results = await asyncio.gather(task_ok(), task_error())
        print("结果:", results)
    except ValueError as e:
        print("成功捕获到异常:", e)

    print("\n--- 测试 return_exceptions=True ---")
    results = await asyncio.gather(task_ok(), task_error(), return_exceptions=True)
    print("结果列表:", results)
    for i, res in enumerate(results):
        if isinstance(res, Exception):
            print(f"索引 {i} 是异常: {res}")
        else:
            print(f"索引 {i} 是正常结果: {res}")


if __name__ == "__main__":
    asyncio.run(main())
