#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
测试同步代码通过 yield 实现协作式多任务
"""


from simple_asyncio import run, sleep, get_event_loop, yield_control, gather


def cpu_heavy_task(task_id, iterations=100000):
    """
    同步 CPU 密集型任务，通过 yield_control() 让出控制权

    Args:
        task_id: 任务 ID
        iterations: 每次迭代的计算量
    """
    print(f"[Task {task_id}] 开始执行")

    for step in range(5):
        # 模拟 CPU 密集型计算
        result = 0
        for i in range(iterations):
            result += i * i

        print(f"[Task {task_id}] 步骤 {step + 1}/5 完成 (result={result})")

        # ⚠️ 关键：yield yield_control() 让出控制权给其他任务
        yield yield_control()

    print(f"[Task {task_id}] ✅ 完成")
    return f"Task {task_id} 结果"


async def async_io_task(task_id):
    """异步 I/O 任务"""
    print(f"[Async {task_id}] 开始执行")
    await sleep(0.1)
    print(f"[Async {task_id}] ✅ 完成")
    return f"Async {task_id} 结果"


async def test_cooperative_multitasking():
    """测试协作式多任务"""
    print("=" * 70)
    print("测试: 同步生成器 + 异步协程混合执行")
    print("=" * 70)

    loop = get_event_loop()

    # 创建多个同步生成器任务
    sync_task1 = loop.create_task(cpu_heavy_task(1, iterations=50000))
    sync_task2 = loop.create_task(cpu_heavy_task(2, iterations=50000))

    # 创建异步 I/O 任务
    async_task1 = loop.create_task(async_io_task("A"))
    async_task2 = loop.create_task(async_io_task("B"))

    print("\n所有任务已启动，观察交错执行...\n")

    # 等待所有任务完成
    results = await gather(sync_task1, sync_task2, async_task1, async_task2)

    print("\n" + "=" * 70)
    print("所有任务完成！")
    print("=" * 70)
    for i, result in enumerate(results, 1):
        print(f"  任务 {i}: {result}")


if __name__ == "__main__":
    run(test_cooperative_multitasking())
