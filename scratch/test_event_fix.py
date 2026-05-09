import os
import sys

# 将当前目录添加到路径中以便导入 simple_asyncio
sys.path.insert(0, os.getcwd())

from simple_asyncio import run, sleep, Event, create_task


async def test_event_clear():
    ev = Event()
    print("Setting event...")
    ev.set()
    print(f"Is set: {ev.is_set()}")

    print("Clearing event...")
    ev.clear()
    print(f"Is set: {ev.is_set()}")

    async def delayed_set():
        await sleep(0.5)
        print("Delayed setting event...")
        ev.set()

    print("Starting delayed set task...")
    create_task(delayed_set())

    print("Waiting on cleared event (should block for 0.5s)...")
    import time

    start = time.time()
    await ev.wait()
    end = time.time()
    print(f"Wait returned in {end - start:.4f}s")

    if end - start >= 0.4:
        print("SUCCESS: Event.clear() fix verified!")
    else:
        print("FAILURE: Wait returned too early!")


if __name__ == "__main__":
    run(test_event_clear())
