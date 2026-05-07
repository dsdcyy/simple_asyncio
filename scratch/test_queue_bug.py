import sys
import os

# 将当前目录添加到路径中以便导入 simple_asyncio
sys.path.insert(0, os.getcwd())

from simple_asyncio import run, sleep, AsyncQueue, create_task


async def test_queue_lost_item():
    q = AsyncQueue(maxsize=1)

    # 1. 让一个 get 等待 (由于队列为空)
    get_task = create_task(q.get())
    await sleep(0.01)  # 确保 task 运行并进入等待

    # 2. 取消 get_task
    get_task.cancel()
    await sleep(0.01)

    # 3. 此时 _getters 里有一个已完成(取消)的 Future
    # 4. 调用 put_nowait。由于 _getters 不为空，它会调用 _wake_getter
    # 根据现有的 _wake_getter 代码:
    # def _wake_getter(self, item: _T) -> bool:
    #     if self._getters:
    #         getter = self._getters.popleft()
    #         if not getter.done():
    #             getter.set_result(item)
    #             return True
    #     return False
    # 它会弹出取消的 getter，看到 done()=True，返回 False。
    # 然后 put_nowait 会执行 self._items.append(item)。这步还没丢数据。

    q.put_nowait("item1")
    print(f"Queue size after put_nowait: {q.qsize()}")  # 应该是 1

    # 现在构造丢数据的场景: _wake_putter
    # 1. 队列已满 (maxsize=1)
    # 2. 让一个 put 等待
    put_task = create_task(q.put("item2"))
    await sleep(0.01)
    # 此时 _putters 有一个 ("item2", fut)

    # 3. 让一个 get 等待并取消它
    get_task2 = create_task(q.get())
    await sleep(0.01)
    get_task2.cancel()
    await sleep(0.01)
    # 此时 _getters 有一个已取消的 Future

    # 4. 调用 get_nowait()。它会调用 _wake_putter()
    # 根据现有的 _wake_putter 代码:
    # def _wake_putter(self):
    #     item, fut = self._putters.popleft()
    #     if self._getters:
    #         getter = self._getters.popleft()
    #         if not getter.done():
    #             getter.set_result(item)
    #     else:
    #         self._items.append(item)
    # 这里丢了！因为 self._getters 不为空，它弹出了取消的 getter，
    # 发现 getter.done()，什么都不做。item2 就这样消失了！

    print(f"Getting item1: {q.get_nowait()}")
    print(f"Queue size after get_nowait (should be 1 if item2 was put): {q.qsize()}")
    if q.qsize() == 0:
        print("BUG CONFIRMED: item2 was LOST in _wake_putter!")


if __name__ == "__main__":
    run(test_queue_lost_item())
