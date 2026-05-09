# Simple Asyncio - 微型异步事件循环框架

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

一个**从零实现**的简化版 asyncio 框架，深入理解 Python 异步编程的核心机制。

---

## 📖 项目简介

`simple_asyncio` 是一个教育性质的微型异步事件循环实现，完整复刻了 Python `asyncio` 的核心功能：

- ✅ **事件循环** - 基于 `selectors` 的 I/O 多路复用
- ✅ **Future/Task 系统** - 协程调度和状态管理
- ✅ **同步原语** - `Lock`, `Semaphore`, `Event`, `Queue`
- ✅ **高级策略锁** - `SelectiveLock`, `CountdownLock`, `ToggleLock`
- ✅ **异步工具** - `sleep`, `gather`, `wait`, `wait_for`
- ✅ **异步 Socket** - 非阻塞网络编程支持
- ✅ **协作式多任务** - 同步生成器通过 `yield_control()` 参与调度

> 🎯 **目标**: 通过阅读源码，彻底理解异步编程的底层原理（事件循环、协程调度、I/O 多路复用等）。

---

## 🚀 快速开始

### 安装

无需安装！直接导入即可使用：

```python
import sys
sys.path.insert(0, '/path/to/simple_asyncio')

from simple_asyncio import run, sleep, gather
```

### 基础示例

#### 1. 运行异步协程

```python
from simple_asyncio import run, sleep

async def hello():
    print("Hello")
    await sleep(1)  # 异步睡眠 1 秒
    print("World!")
    return "done"

result = run(hello())
print(f"Result: {result}")
```

#### 2. 并发执行多个任务

```python
from simple_asyncio import run, sleep, gather

async def task(name, delay):
    print(f"[{name}] 开始")
    await sleep(delay)
    print(f"[{name}] 完成")
    return f"{name} 结果"

# 并发执行 3 个任务
results = run(gather(
    task("A", 0.5),
    task("B", 0.3),
    task("C", 0.7)
))

print(f"所有结果: {results}")
```

#### 3. 同步代码参与异步调度

```python
from simple_asyncio import run, sleep, yield_control, get_event_loop,gather

def cpu_heavy_task(task_id):
    """同步 CPU 密集型任务，通过 yield_control() 让出控制权"""
    for step in range(5):
        # 模拟计算
        result = sum(i * i for i in range(10000))
        print(f"[Task {task_id}] 步骤 {step + 1}/5, 计算结果: {result}")
        
        # ⚠️ 关键：让出控制权给其他任务
        yield yield_control()
    
    return f"Task {task_id} 完成,"

async def io_task(name):
    """异步 I/O 任务"""
    print(f"[{name}] 开始 I/O")
    await sleep(0.1)
    print(f"[{name}] I/O 完成")
    return f"{name} 结果"

loop = get_event_loop()

# 混合执行同步和异步任务
sync_task = loop.create_task(cpu_heavy_task(1))
async_task = loop.create_task(io_task("IO"))

results = run(gather(sync_task, async_task))
print(f"结果: {results}")
```

---

## 📚 核心组件

### 1. EventLoop - 事件循环

事件循环是整个框架的核心，负责：
- 调度和管理异步任务
- 处理 I/O 多路复用（基于 `selectors`）
- 管理定时器
- 提供线程安全的回调调度

```python
from simple_asyncio import get_event_loop,sleep

loop = get_event_loop()

# 创建任务
async def my_coro():
    await sleep(1)
    return "done"

task = loop.create_task(my_coro(), name="MyTask")
loop.run_until_complete(task)
print(task.result())

loop.close()
```

### 2. Future & Task - 异步原语

**Future**: 代表尚未完成的异步操作结果。

```python
from simple_asyncio import Future, get_event_loop

loop = get_event_loop()
future = loop.create_future()

# 设置结果
future.set_result("hello")

# 获取结果
print(future.result())  # "hello"

# 取消 Future
future.cancel(msg="用户取消")
print(future.cancelled())  # True
print(future.cancel_msg())  # "用户取消"
```

**Task**: 包装协程/生成器，驱动其一步步执行。

```python
from simple_asyncio import Task, get_event_loop,sleep

async def my_coro():
    await sleep(1)
    return "done"

loop = get_event_loop()
task = loop.create_task(my_coro(), name="MyTask")

# 检查状态
print(task.done())      # False
print(task.cancelled()) # False

# 取消任务
task.cancel(msg="测试取消")
print(task.cancelled())  # True
```

### 3. TimerHandle - 定时器管理

```python
from simple_asyncio import sleep, get_running_loop, run


async def main():
    loop = get_running_loop()

    def callback(value):
        print(f"定时器触发: {value}")

    # 创建定时器（0.5 秒后执行）
    handle = loop.call_later(0.5, callback, "hello")

    # 取消定时器
    handle.cancel()

    await sleep(1)  # 等待观察


run(main())
```

### 4. 线程安全调度

```python
import threading

from simple_asyncio import get_running_loop, sleep, run


async def main():
    loop = get_running_loop()

    def thread_func():
        """从其他线程向事件循环提交回调"""
        print("[Thread] 提交回调...")
        loop.call_soon_threadsafe(lambda: print("[EventLoop] 回调执行"))

    # 启动线程
    thread = threading.Thread(target=thread_func)
    thread.start()
    thread.join()

    await sleep(0.1)  # 等待回调执行


run(main())
```

### 5. 异步 Socket

```python
from simple_asyncio import AsyncSocket, run
import socket

async def tcp_client():
    sock = AsyncSocket(socket.socket())
    await sock.connect(('127.0.0.1', 8080))
    
    await sock.send(b"Hello Server")
    data = await sock.recv(1024)
    
    print(f"收到: {data.decode()}")
    sock.close()

run(tcp_client())
```

### 注册 I/O 回调（`add_reader` / `add_writer`）

`EventLoop` 的 I/O 注册支持在同一文件描述符上同时注册多个读/写回调，并区分一次性（one-shot）与持久回调。

- `one_shot=False`（默认）: 回调在事件发生后保持注册，除非回调内部或外部调用 `remove_reader`/`remove_writer`。
- `one_shot=True`: 回调在首次触发后自动移除（一次性回调）。

示例：一次性读回调

```python
from simple_asyncio import get_event_loop

def on_read():
    data = sock.recv(1024)
    print('收到数据:', data)

loop = get_event_loop()
# 注册一次性读回调（只在下一次可读时触发）
loop.add_reader(sock, on_read, one_shot=True)
```

如果希望在回调内继续监听，请在回调结束时重新调用 `add_reader` 或使用持久回调（默认行为）。


### 线程安全的 I/O 注册与按回调移除

库提供了对 I/O 回调更细粒度的控制：

- **线程安全注册**：`add_reader_threadsafe(fileobj, callback, *args, one_shot=False)` 和 `add_writer_threadsafe(...)`。
  这些方法可以在非事件循环线程中安全调用。它们内部会使用 `call_soon_threadsafe` 将实际注册调度到事件循环线程并唤醒循环。

示例：

```python
import threading
from simple_asyncio import get_event_loop

loop = get_event_loop()

def cb(fut):
    fut.set_result('ok')

f = loop.create_future()

def worker():
    # 在另一个线程中注册可读回调（安全）
    loop.add_reader_threadsafe(sock, cb, f)

threading.Thread(target=worker).start()
res = loop.run(f)
```

- **按回调移除**：如果你在同一文件描述符上注册了多个回调，可以使用 `remove_reader_callback(fileobj, callback)` 或 `remove_writer_callback(fileobj, callback)` 精确移除指定回调（按回调函数对象匹配，非线程安全）。对应的线程安全版本为 `remove_reader_callback_threadsafe(...)` 和 `remove_writer_callback_threadsafe(...)`，它们会将移除操作调度回事件循环线程。

示例：

```python
def on_read_a():
    print('A')

def on_read_b():
    print('B')

loop.add_reader(sock, on_read_a)
loop.add_reader(sock, on_read_b)

# 仅移除 on_read_a
loop.remove_reader_callback(sock, on_read_a)
```

注意事项：

- `remove_*_callback` 默认按函数对象 (`is`) 匹配并移除所有相同函数的条目；如果你需要按参数精确匹配，可在回调封装时保存引用或请求库支持按 `(callback, args)` 匹配。
- 线程安全方法是通过将操作调度回事件循环线程实现的，因此它们返回时不保证已立即生效：若需要确认已注册/移除，可将操作与一个 Future/事件配合使用。


---

## 🔐 同步与通信

库提供了一套完整的异步同步原语，用于协程间的状态同步和数据传递。

### 1. 基础锁与信号量
支持经典的 `async with` 语法。

```python
from simple_asyncio import AsyncLock, AsyncSemaphore, run, gather, sleep

async def worker(name, lock):
    async with lock:
        print(f"{name} 获得了锁")
        await sleep(1)
    print(f"{name} 释放了锁")

async def main():
    # 互斥锁
    lock = AsyncLock()
    # 信号量（并发度为 2）
    sem = AsyncSemaphore(2)
    
    await gather(worker("A", lock), worker("B", lock))

run(main())
```

### 2. 事件同步 (Event)

```python
from simple_asyncio import Event, run, sleep, gather

async def waiter(event):
    print("等待事件触发...")
    await event.wait()
    print("事件已触发，继续执行！")

async def setter(event):
    await sleep(2)
    print("触发事件！")
    event.set()

run(gather(waiter(event := Event()), setter(event)))
```

### 3. 异步队列 (AsyncQueue)

```python
from simple_asyncio import AsyncQueue, run, sleep, create_task, gather


async def producer(q):
   for i in range(5):
      await q.put(f"数据-{i}")
      await sleep(0.5)


async def consumer(q):
   while True:
      item = await q.get()
      print(f"消费: {item}")
      if item == "数据-4": break


async def main():
   q = AsyncQueue()
   # 启动消费者
   consumer_task = create_task(consumer(q))
   # 启动生产者
   producer_task = create_task(producer(q))
   # 等待所有任务完成
   await gather(consumer_task, producer_task)


run(main())
```

### 4. 高级策略锁 (SelectiveLock)
允许你等待一组任务中的**特定子集**，而无需等待全部。

```python
from simple_asyncio import AsyncSelectiveLock, run, sleep, gather

async def main():
    lock = AsyncSelectiveLock()
    
    # 领票执行
    async with lock as tid1: # ID: 1
        await sleep(1)
        
    async with lock as tid2: # ID: 2
        await sleep(5)

    # 局部等待：只等 ID 1 完成，不管 ID 2
    await lock.wait_unlock(target_ids=[1])
    print("ID 1 已完成，流程继续")

run(main())
```

### 5. 异步计数屏障 (CountdownLock / WaitGroup)
支持类似 Go `WaitGroup` 的动态计数，也支持类似 Java `CountDownLatch` 的固定倒计时。

```python
from simple_asyncio import AsyncCountdownLock, run, sleep, create_task, gather

async def worker(lock, name):
    await sleep(1)
    print(f"{name} 完成")
    lock.release()

async def main():
    # 初始化计数为 3 (CountDownLatch 模式)
    lock = AsyncCountdownLock(count=3)
    # 后台启动 3 个并发任务
    for name in ["A", "B", "C"]:
        _ = create_task(worker(lock, name))
        
    print("主流程阻塞中，等待所有计数归零...")
    await lock.wait_unlock()
    print("✅ 所有任务已完成，主流程放行！")

run(main())
```

### 6. 开关锁 (ToggleLock)
支持手动暂停和恢复的全局开关，非常适合做全局的暂停/继续流控。

```python
from simple_asyncio import AsyncToggleLock, run, sleep, get_running_loop

async def job(lock, i):
    await lock.wait_unlock()  # 检查开关状态
    print(f"执行任务 {i}")

async def controller(lock: AsyncToggleLock):
    print("🚫 系统暂停执行")
    lock.deactivate() 
    await sleep(2)
    print("✅ 系统恢复执行")
    lock.activate() 

async def main():
    lock = AsyncToggleLock()
    
    # 立即触发几个任务，但它们会被控制器卡住
    for i in range(3):
        _ = get_running_loop().create_task(job(lock, i))
        
    await controller(lock)

run(main())
```

## 🎯 高级特性

### ContextVar 上下文隔离

使用 `ContextVar` 存储当前事件循环，确保：
- ✅ 线程安全
- ✅ 协程安全
- ✅ 防止嵌套调用冲突

```python
from simple_asyncio import run, get_running_loop, _loop_var

async def check_context():
    loop = get_running_loop()
    print(f"当前循环: {id(loop)}")
    return loop

# 第一个循环
loop1 = run(check_context())

# 第二个循环（完全独立）
loop2 = run(check_context())

assert id(loop1) != id(loop2)  # 不同的循环实例
```

### 任务取消与传播

```python
from simple_asyncio import run, sleep, FutureCancelledError, get_running_loop


async def long_task():
   try:
      print("开始长任务...")
      await sleep(10)
      return "完成"
   except FutureCancelledError as e:
      print(f"任务被取消: {e.cancel_msg()}")
      raise  # 必须重新抛出


task = None


async def main():
   global task
   task = get_running_loop().create_task(long_task())

   await sleep(0.5)
   task.cancel(msg="超时取消")

   try:
      await task
   except FutureCancelledError:
      print("已捕获取消异常")


run(main())
```

### 超时控制

```python
from simple_asyncio import run, sleep, wait_for, AsyncioTimeoutError


async def slow_task():
   await sleep(5)
   return "完成"


try:
   # 设置 1 秒超时
   result = run(wait_for(slow_task(), timeout_delay=1.0))
except AsyncioTimeoutError:
   print("任务超时！")
```

---

## 📊 架构设计

### 核心流程图

```
┌─────────────────────────────────────────────┐
│              EventLoop                       │
│                                              │
│  ┌──────────┐    ┌──────────┐               │
│  │ _ready   │───▶│ _step()  │               │
│  │ (deque)  │    │ (Task)   │               │
│  └──────────┘    └──────────┘               │
│       ▲                │                     │
│       │                ▼                     │
│  ┌──────────┐    ┌──────────┐               │
│  │Selector  │◀───│ yield    │               │
│  │(I/O)     │    │ Future   │               │
│  └──────────┘    └──────────┘               │
│                                              │
│  ┌──────────┐                               │
│  │ Timers   │──▶ call_later()               │
│  │ (heapq)  │                               │
│  └──────────┘                               │
└─────────────────────────────────────────────┘
```

### 关键设计决策

| 特性 | 实现方式 | 优势 |
|------|---------|------|
| **事件循环** | `selectors.select()` | 跨平台 I/O 多路复用 |
| **线程安全** | `socketpair` + `Lock` | 高效唤醒阻塞的 selector |
| **上下文管理** | `ContextVar` | 自动继承和恢复 |
| **定时器** | `heapq` 最小堆 | O(log n) 插入/删除 |
| **任务调度** | `deque` FIFO | O(1) 入队/出队 |

---

## 🧪 测试

项目包含完整的测试套件：

```bash
cd  simple_asyncio

# 运行所有测试
python test/test_future_cancel.py
python test/test_task_cancel.py
python test/test_sync_generator.py
python test/test_timer_handle.py
python test/test_contextvar.py
python test/test_eventloop_run_contextvar.py
```

### 测试覆盖

- ✅ Future 取消和消息传递
- ✅ Task 命名和取消传播
- ✅ 同步生成器协作式多任务
- ✅ TimerHandle 创建、取消和执行
- ✅ ContextVar 上下文隔离
- ✅ EventLoop.run() 上下文设置

---

## 📖 学习路径

如果你想深入理解异步编程，建议按以下顺序阅读源码：

1. **Future 类** (第 150-280 行)
   - 理解异步结果的表示和状态管理
   
2. **Task 类** (第 320-500 行)
   - 理解协程如何被驱动执行
   
3. **EventLoop._run_once()** (第 800-900 行)
   - 理解事件循环的核心调度逻辑
   
4. **call_soon_threadsafe()** (第 680-700 行)
   - 理解线程安全调度的实现原理
   
5. **TimerHandle** (第 511-570 行)
   - 理解定时器管理机制

---

## 🔍 与标准库 asyncio 对比

| 特性 | simple_asyncio | asyncio |
|------|---------------|---------|
| **事件循环** | ✅ 基于 selectors | ✅ 基于 epoll/kqueue/IOCP |
| **Future/Task** | ✅ 完整实现 | ✅ 完整实现 |
| **定时器** | ✅ heapq 实现 | ✅ 类似实现 |
| **线程安全** | ✅ socketpair | ✅ self-pipe trick |
| **ContextVar** | ✅ 完整支持 | ✅ 完整支持 |
| **原生协程** | ✅ 支持 | ✅ 支持 |
| **生成器协程** | ✅ 支持 | ⚠️ 已弃用 |
| **性能优化** | ❌ 无 | ✅ C 扩展优化 |
| **生产就绪** | ❌ 教育用途 | ✅ 生产级 |

> 💡 **关键区别**: `simple_asyncio` 是纯 Python 实现，用于学习；`asyncio` 有 C 扩展优化，用于生产。

---

## 🛠️ 开发指南

### 添加新功能

1. **添加新的异步原语**
   ```python
   from simple_asyncio import Future, get_running_loop, sleep
   def my_new_primitive(arg):
       loop = get_running_loop()
       future = loop.create_future()
       # ... 实现逻辑
       return future
   ```

2. **扩展 EventLoop**
   ```python
   class EventLoop:
       def my_custom_method(self):
           # ... 自定义逻辑
           pass
   ```

3. **添加测试**
   ```python
   # test/test_my_feature.py
   from simple_asyncio import run
   def my_async_func():
        yield "expected"
   def test_my_feature():
       result = run(my_async_func())
       assert result == "expected"
   ```

---

## 📝 常见问题

### Q1: 为什么需要 `yield_control()`？

**A**: 允许同步生成器参与异步调度，实现协作式多任务。适用于 CPU 密集型任务需要定期让出控制权的场景。

```python
from simple_asyncio import yield_control

def cpu_task():
    for i in range(1000000):
        # 计算...
        if i % 10000 == 0:
            yield yield_control()  # 让出控制权
```

### Q2: `call_soon_threadsafe` 的原理是什么？

**A**: 使用两个机制保证线程安全：
1. **Lock**: 保护 `_ready` 队列的并发访问
2. **socketpair**: 写入一个字节打断 `selector.select()` 的阻塞

```python
def call_soon_threadsafe(self, callback, *args):
    with self._ready_lock:
        self._ready.append((callback, args))
    self._wake_loop()  # 写入 socketpair
```

### Q3: 为什么使用 ContextVar 而不是全局变量？

**A**: ContextVar 提供：
- ✅ **线程安全**: 每个线程独立的上下文
- ✅ **协程安全**: 子协程自动继承父协程的上下文
- ✅ **自动恢复**: `reset(token)` 确保上下文正确清理

### Q4: 可以用于生产环境吗？

**A**: ❌ **不可以**。这是教育项目，缺少：
- 性能优化（C 扩展）
- 完善的错误处理
- 全面的测试覆盖
- 长期维护承诺

请使用标准库 `asyncio` 进行生产开发。

---

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

### 贡献指南

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交更改 (`git commit -m 'Add amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 开启 Pull Request

---

## 📄 许可证

MIT License

---

## 🙏 致谢

- Python 官方 `asyncio` 模块 - 设计灵感来源
- David Beazley 的协程教程 - 深入理解生成器协程
- Real Python 异步编程文章 - 最佳实践参考

---

## 📧 联系方式

- **Author**: dsdcyy
- **Created**: 2026/5/4
- **Project**: simple_asyncio

---

> 💡 **提示**: 这个项目的主要目的是**学习**，不是替代 `asyncio`。通过阅读源码，你将深刻理解异步编程的底层机制！
