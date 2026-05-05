#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Author : Ljw
# @Time : 2026/5/4 18:15
# @FileName  :simple_asyncio.py

import errno
import heapq
import os
import selectors
import socket
import threading
import time
import weakref
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from types import GeneratorType, CoroutineType
from typing import Generator, Awaitable, Any

current_loop: "EventLoop|None" = None

FIRST_COMPLETED = "FIRST_COMPLETED"
FIRST_EXCEPTION = "FIRST_EXCEPTION"
ALL_COMPLETED = "ALL_COMPLETED"


def get_running_loop():
    if current_loop is None:
        raise RuntimeError("No running event loop")
    return current_loop


def get_event_loop():
    global current_loop
    if current_loop is None:
        current_loop = EventLoop()
    return current_loop


class CancelledError(Exception):
    pass


class TimeoutError(Exception):
    pass


class YieldControl:
    """用于同步生成器让出控制权的标记对象"""
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance


def yield_control():
    """
    在同步生成器中调用，让出控制权给其他任务
    
    用法:
        def sync_task():
            # 做一些工作
            yield yield_control()  # 让出控制权
            # 继续工作
    """
    return YieldControl()


class Future:
    __slots__ = (
        '_result', '_exception', '_done', '_cancelled',
        '_cancel_msg', '_callbacks', '_loop'
    )

    def __init__(self, loop=None):
        self._result = None
        self._exception = None
        self._done = False
        self._cancelled = False
        self._cancel_msg = None
        self._callbacks = []
        self._loop = loop or get_running_loop()

    def _check_done(self, check_true=True):
        if self._done is check_true:
            if check_true:
                raise RuntimeError("Future already done")
            raise RuntimeError("Future is not done")

    def _finish(self, result=None, exc=None):
        self._check_done()
        self._result = result
        self._exception = exc
        self._done = True
        callbacks = self._callbacks
        self._callbacks = []
        for cb in callbacks:
            self._loop.call_soon(cb, self)

    def set_result(self, result):
        self._finish(result)

    def set_exception(self, exc):
        self._finish(exc=exc)

    def add_done_callback(self, cb):
        if self._done:
            self._loop.call_soon(cb, self)
        else:
            self._callbacks.append(cb)

    def done(self):
        return self._done

    def exception(self):
        self._check_done(False)
        return self._exception

    def result(self):
        self._check_done(False)
        if self._exception is not None:
            raise self._exception
        return self._result

    def __iter__(self):
        """让 future 可被 yield，生成器通过 yield future 来等待"""
        result = yield self
        return result

    def __await__(self):
        """让 future 可被 await，支持原生协程"""
        # __await__ 必须返回一个迭代器
        return self.__iter__()

    def cancel(self, msg=None):
        """
        取消 Future
        
        Args:
            msg: 取消原因消息（可选）
        
        Returns:
            bool: 是否成功取消
        """
        if self._done or self._cancelled:
            return False

        self._cancelled = True
        self._cancel_msg = msg
        self._finish(exc=CancelledError(msg))
        return True

    def cancelled(self):
        """检查 Future 是否被取消"""
        return self._cancelled

    def cancel_msg(self):
        """获取取消时的消息"""
        return self._cancel_msg


class Task(Future):
    """把一个生成器包装成 Future，驱动它一步步执行"""
    __slots__ = (
        'gen', 'name', '_next_value', '_stepping',
        '_rescheduled', '_waiting'
    )

    def __init__(self, gen: Awaitable | Generator, loop=None, name=None):
        super().__init__(loop)
        self.gen = gen
        self.name = name or f"Task-{id(self)}"  # 任务名称
        # 启动生成器，并处理它遇到的第一个 yield
        self._loop.call_soon(self._step)
        self._next_value = None
        self._stepping = False
        self._rescheduled = False
        self._waiting = None  # 当前等待的 Future

    def _on_done(self, f):
        if self._done:
            return
        if self._waiting is not f:
            return
        self._waiting = None
        if self._cancelled:
            # 直接走取消流程，不取结果
            self._loop.call_soon(self._step)
            return
        try:
            res = f.result()
        except Exception as e:
            self._loop.call_soon(self._step, e, True)
        else:
            self._next_value = res
            self._loop.call_soon(self._step)

    def cancelled(self):
        """检查 Task 是否被取消（基于异常类型判断）"""
        return self._done and isinstance(self._exception, CancelledError)

    def cancel(self, msg=None):
        """
        取消任务8
        
        Args:
            msg: 取消原因消息（可选）
        
        Returns:
            bool: 是否成功取消
        """
        if self._done or self._cancelled:
            return False

        self._cancelled = True
        self._cancel_msg = msg

        if self._waiting and hasattr(self._waiting, "cancel"):
            self._waiting.cancel()

        self._waiting = None
        self._loop.call_soon(self._step)
        return True

    def _step(self, value=None, is_exc=False):
        if self._done:
            return

        if self._stepping:
            # 不丢弃，而是“稍后再试”
            if not self._rescheduled:
                self._rescheduled = True
                self._loop.call_soon(self._step, value, is_exc)
            return

        self._stepping = True

        try:
            if is_exc:
                yielded = self.gen.throw(value)

            elif self._cancelled:
                self._cancelled = False
                self._next_value = None  # 清掉 resume 值（关键！）
                yielded = self.gen.throw(CancelledError(self._cancel_msg))

            else:
                send_value = self._next_value
                self._next_value = None
                yielded = self.gen.send(send_value)
        except StopIteration as e:
            self.set_result(e.value)  # 正常结束，返回值作为结果
            return
        except Exception as e:
            self.set_exception(e)
            return
        finally:
            self._stepping = False
            self._rescheduled = False
        # 如果生成器 yield 了一个 Future，就等它完成后继续 _step
        if isinstance(yielded, Future):
            self._waiting = yielded
            yielded.add_done_callback(self._on_done)
        # ✅ 合并判断：生成器协程和原生协程使用相同的处理逻辑
        elif isinstance(yielded, (GeneratorType, CoroutineType)):
            yielded = self._loop.create_task(yielded)
            self._waiting = yielded
            yielded.add_done_callback(self._on_done)
        # ✅ 支持同步生成器通过 yield_control() 让出控制权
        elif isinstance(yielded, YieldControl):
            # 创建一个立即完成的 Future，让生成器在下一次循环中恢复
            immediate_future = self._loop.create_future()
            self._loop.call_soon(immediate_future.set_result, None)
            self._waiting = immediate_future
            immediate_future.add_done_callback(self._on_done)
        else:
            # 对于普通值，将其作为下一次恢复执行时的输入，并排期到下一轮循环
            self._next_value = yielded
            self._loop.call_soon(self._step)


class TimerHandle:
    """代表一个已排期的定时器回调，可以取消"""
    __slots__ = ('_callback', '_args', '_loop_ref', '_when', '_cancelled')

    def __init__(self, callback, args, loop, when):
        self._callback = callback
        self._args = args
        self._loop_ref = weakref.ref(loop)  # 弱引用

        self._when = when  # 记录到期时间，便于调试
        self._cancelled = False

    def cancel(self):
        """取消定时器，后续到期时其回调将不会执行"""
        self._cancelled = True

    def run(self):
        """将回调排入就绪队列（与老行为一致）"""
        if not self._cancelled:
            loop = self._loop_ref()
            if loop:
                loop.call_soon(self._callback, *self._args)

    def cancelled(self):
        """检查定时器是否已被取消"""
        return self._cancelled

    def __repr__(self):
        status = "cancelled" if self._cancelled else "scheduled"
        return f"<TimerHandle when={self._when:.3f} {status}>"

    def when(self):
        """返回定时器的到期时间（单调时间）"""
        return self._when


class EventLoop:
    def __init__(self):
        self._ready = deque()  # 就绪回调队列
        self._scheduled = []  # 最小堆，存 (deadline, seq, callback, args)
        self._timer_seq = 0
        self._running = False
        self._tasks: set[Task] = set()
        self._selector = selectors.DefaultSelector()
        self._exception_handler = None
        self._execute_pool = None
        self._ready_lock = threading.Lock()  # 保护 _ready 的线程锁
        self._closed = False  # 标记是否已关闭

        # 创建自唤醒管道
        self._self_reader, self._self_writer = socket.socketpair()
        self._self_reader.setblocking(False)
        self._self_writer.setblocking(False)

        # 将读端注册到 selector，回调用于“消费”唤醒信号
        self._selector.register(
            self._self_reader.fileno(),
            selectors.EVENT_READ,
            (self._consume_wake, ())
        )

    def is_closed(self):
        return self._closed

    def close(self):
        if self._closed:
            return  # 已经关闭，避免重复关闭
        self._closed = True
        try:
            self._selector.unregister(self._self_reader.fileno())
        except KeyError:
            pass  # 可能已经注销

        self._self_reader.close()
        self._self_writer.close()
        self._selector.close()

        # 关闭线程池
        if self._execute_pool is not None:
            self._execute_pool.shutdown(wait=False)
            self._execute_pool = None

    def _consume_wake(self):
        """读取自唤醒管道中的字节（消费信号）"""
        try:
            while True:
                self._self_reader.recv(4096)
        except BlockingIOError:
            pass  # 读空为止
        except OSError:
            pass

    def call_soon_threadsafe(self, callback, *args):
        """线程安全版本的 call_soon，供其他线程调用"""
        with self._ready_lock:
            self._ready.append((callback, args))
        self._wake_loop()

    def _wake_loop(self):
        """向自唤醒管道写入一个字节，打断 selector.select"""
        try:
            self._self_writer.send(b'\x00')
        except OSError:
            pass  # 写端已关闭等异常忽略

    def run_in_thread_executor(self, func, *args, **kwargs):
        """在线程池中异步执行函数，返回结果"""
        if self._execute_pool is None:
            self._execute_pool = ThreadPoolExecutor()

        tf = self._execute_pool.submit(func, *args, **kwargs)
        f = self.create_future()

        # 支持取消：当 Future 被取消时，也取消线程任务
        def cancel_thread_future(fut):
            if fut.cancelled() and not tf.done():
                tf.cancel()

        f.add_done_callback(cancel_thread_future)

        def _on_thread_done():
            def set_result():
                try:
                    res = tf.result()
                except Exception as e:
                    f.set_exception(e)
                else:
                    f.set_result(res)

            self.call_soon_threadsafe(set_result)

        tf.add_done_callback(lambda _: _on_thread_done())
        return f

    def set_exception_handler(self, handler):
        """设置自定义异常处理器，handler 支持可调用对象，传入 (loop, context)"""
        if handler is not None and not callable(handler):
            raise TypeError("handler must be callable or None")
        self._exception_handler = handler

    def call_exception_handler(self, context):
        """调用用户设置的自定义异常处理器，或默认打印"""
        if self._exception_handler is not None:
            try:
                self._exception_handler(self, context)
            except Exception as e:
                # 处理器自身出错，回退到默认打印
                print(f"Exception in exception handler: {e}")
                print(f"Original context: {context}")
        else:
            # 默认行为：简单打印（可根据需要输出到 sys.stderr）
            print(f"[loop] Exception: {context.get('message')}")
            exc = context.get('exception')
            if exc:
                import traceback
                traceback.print_exception(type(exc), exc, exc.__traceback__)

    def call_soon(self, callback, *args):
        """立即安排回调"""
        with self._ready_lock:
            self._ready.append((callback, args))

    def call_later(self, delay, callback, *args):
        """延迟 delay 秒后执行回调"""
        deadline = time.monotonic() + delay
        self._timer_seq += 1
        handler = TimerHandle(callback, args, self, deadline)
        heapq.heappush(self._scheduled, (deadline, self._timer_seq, handler))
        return handler

    def add_reader(self, fileobj, callback, *args):
        try:
            self._selector.unregister(fileobj)
        except KeyError:
            pass
        self._selector.register(fileobj, selectors.EVENT_READ, (callback, args))

    def add_writer(self, fileobj, callback, *args):
        try:
            self._selector.unregister(fileobj)
        except KeyError:
            pass
        self._selector.register(fileobj, selectors.EVENT_WRITE, (callback, args))

    def remove_reader(self, fileobj):
        """移除文件描述符的可读事件"""
        try:
            self._selector.unregister(fileobj)
        except KeyError:
            pass  # 已经移除

    def remove_writer(self, fileobj):
        """移除文件描述符的可写事件"""
        try:
            self._selector.unregister(fileobj)
        except KeyError:
            pass  # 已经移除

    def create_task(self, gen: Generator[Future, Any, Any] | Awaitable, name=None):
        """
        接收生成器 coroutine，返回 Task
        
        Args:
            gen: 生成器或协程
            name: 任务名称（可选）
        """
        task = Task(gen, self, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._task_done)
        return task

    def _task_done(self, task: Task):
        self._tasks.discard(task)

    def create_future(self):
        return Future(self)

    def run(self, awaitable: Generator[Future, Any, Any] | Task | Future):
        """
        直接运行一个 awaitable（生成器/Task/Future）并返回最终结果。

        便捷入口，等价于：
            task = loop.create_task(coro_gen)
            loop.run_until_complete(task)
            return task.result()
        """
        if isinstance(awaitable, Task):
            task = awaitable
        elif isinstance(awaitable, Future):
            task = awaitable
        else:
            task = self.create_task(awaitable)
        self.run_until_complete(task)
        return task.result()

    def run_until_complete(self, main_task: Task):
        self._running = True
        main_task.add_done_callback(lambda _: self.stop())
        self._run_once(main_task)  # 主循环（直到主任务完成）

        # 取消所有其他未完成的任务
        self._cancel_other_tasks(main_task)

        if self._tasks:
            self._running = True
            self._run_once(main_task)

    def _complete_a_task(self):

        cb, args = self._ready.popleft()
        try:
            cb(*args)
        except Exception as e:
            # 构建上下文信息
            context = {
                'message': 'Exception in callback',
                'exception': e,
                'callback': cb,
                'args': args,
            }
            self.call_exception_handler(context)

    def _run_once(self, main_task: Task):
        while self._running:
            # 核心退出条件：如果主任务已完成，且没有其他后台任务正在运行（常用于清理阶段）
            if main_task.done() and not self._tasks:
                break

            # 1. 获取当前时间戳（每一轮循环开始时更新）
            now = time.monotonic()

            # 2. 检查定时器是否到期，移到就绪队列
            while self._scheduled and self._scheduled[0][0] <= now:
                _, _, handler = heapq.heappop(self._scheduled)
                if handler.cancelled():
                    continue
                handler.run()
            timeout: float | None = None
            if self._ready:
                timeout = 0.0
            elif self._scheduled:
                timeout = max(self._scheduled[0][0] - now, 0.0)
            # 获取selector的事件列表
            events = self._selector.select(timeout)
            for key, mask in events:
                cb, args = key.data
                self.call_soon(cb, *args)
                self._selector.unregister(key.fileobj)
            # 4. 执行所有就绪队列里的回调
            # 限制执行当前长度的任务，避免 call_soon 导致的无限忙轮询
            with self._ready_lock:
                n = len(self._ready)
            for _ in range(n):
                with self._ready_lock:
                    if not self._ready:
                        break
                self._complete_a_task()

            # 5. 决定是继续干活还是休息
            # 如果已经停止运行（如主任务已完成），或者还有就绪任务，则不再进入休眠
            if not self._running:
                break

            if self._ready:
                continue  # 有就绪任务，继续处理

            # 如果没有就绪任务，且没有定时器
            if not self._scheduled:
                # 检查 IO 状态：如果只剩下自唤醒管道 (Self-Pipe) 还在监听，说明已无实际 IO 任务
                if len(self._selector.get_map()) <= 1:
                    if not main_task.done():
                        raise RuntimeError("Loop stopped before task completed")
                    break

    def _cancel_other_tasks(self, main_task):
        for t in list(self._tasks):
            if t is not main_task:
                t.cancel()

    def stop(self):
        self._running = False


def sleep(delay):
    """返回一个 Future，事件循环在 delay 秒后 set_result"""
    loop = get_running_loop()
    f = loop.create_future()
    handle = loop.call_later(delay, f.set_result, None)

    def on_cancel(fut):
        if fut.cancelled() and not handle.cancelled():
            handle.cancel()

    f.add_done_callback(on_cancel)
    return f


def run(awaitable: Awaitable | Generator[Future, Any, Any]):
    """
    模块级入口，类似 asyncio.run(awaitable)
    每次调用会创建一个新的事件循环，运行完后自动关闭并清理。
    禁止嵌套调用。
    """
    global current_loop

    if current_loop is not None:
        raise RuntimeError("Cannot run the event loop while another is running")

    loop = EventLoop()
    current_loop = loop
    try:
        return loop.run(awaitable)
    finally:
        loop.close()
        current_loop = None


def gather(*coro_s: Awaitable | Generator[Future, Any, Any] | Task):
    """
    并发运行多个生成器，并等待它们全部完成。
    返回一个 Future，其结果是按顺序排列的所有生成器返回值。
    """
    loop = get_running_loop()
    all_done_future = loop.create_future()

    if not coro_s:
        all_done_future.set_result([])
        return all_done_future

    results = [None] * len(coro_s)
    completed_count = 0
    failed = False
    tasks = []

    def _on_task_done(idx, task_f):
        nonlocal completed_count, failed

        if failed:  # 如果已经有一个任务失败了，忽略其他的
            return

        try:
            # 尝试获取结果，如果任务抛异常了，这里会直接 raise
            results[idx] = task_f.result()
            completed_count += 1

            # 如果全部任务都顺利完成了
            if completed_count == len(coro_s):
                all_done_future.set_result(results)

        except Exception as e:
            # 只要有一个任务失败，gather 整体就宣告失败
            failed = True
            for _task in tasks:
                if not _task.done():
                    _task.cancel(msg=f"Gather failed due to task {_task.name}")
            all_done_future.set_exception(e)

    def _propagate_cancel(f):
        try:
            f.result()
        except CancelledError:
            for t in tasks:
                if not t.done():
                    t.cancel(msg="Gather cancelled by parent")

    for i, coro in enumerate(coro_s):
        task = coro
        # 启动协程任务（支持生成器协程和原生协程）
        if isinstance(task, (GeneratorType, CoroutineType)):
            task = loop.create_task(coro)
        # 绑定回调。注意使用闭包捕获当前的索引 i
        task.add_done_callback(lambda tf, idx=i: _on_task_done(idx, tf))
        tasks.append(task)
    all_done_future.add_done_callback(_propagate_cancel)
    return all_done_future


def wait(fs, timeout=None, return_when=ALL_COMPLETED):
    """
    等待一组任务完成。
    返回: (done_set, pending_set)
    """
    loop = get_running_loop()
    wait_future = loop.create_future()

    # 统一转换成 Task 对象
    tasks = []
    for f in fs:
        if isinstance(f, Task):
            tasks.append(f)
        elif isinstance(f, (GeneratorType, CoroutineType)):
            tasks.append(loop.create_task(f))
        else:
            # 也可以支持 Future
            tasks.append(f)

    done = set()
    pending = set(tasks)
    timeout_handle = None

    if not tasks:
        wait_future.set_result((done, pending))
        return wait_future

    def _on_completion(task):
        if wait_future.done():
            return

        pending.remove(task)
        done.add(task)

        # 检查是否满足返回条件
        should_return = False
        if return_when == FIRST_COMPLETED:
            should_return = True
        elif return_when == FIRST_EXCEPTION:
            if task.exception() is not None or not pending:
                should_return = True
        elif return_when == ALL_COMPLETED:
            if not pending:
                should_return = True

        if should_return:
            if timeout_handle and not timeout_handle.cancelled():
                # 任务提前完成了，取消掉超时定时器
                timeout_handle.cancel()
            wait_future.set_result((done, pending))

    # 启动监听
    for t in tasks:
        if t.done():
            _on_completion(t)
        else:
            t.add_done_callback(_on_completion)

    # 处理超时逻辑
    if timeout is not None:
        def _on_timeout():
            if not wait_future.done():
                wait_future.set_result((done, pending))

        timeout_handle = loop.call_later(timeout, _on_timeout)

    return wait_future


def wait_for(aw, timeout):
    """
    等待一个协程或 Future 完成，如果超时则取消并抛出 TimeoutError。
    """
    loop = get_running_loop()
    done_future = loop.create_future()

    # 包装成 Task（支持生成器协程和原生协程）
    if isinstance(aw, (GeneratorType, CoroutineType)):
        task = loop.create_task(aw)
    else:
        task = aw

    def _on_done(f):
        if not done_future.done():
            try:
                done_future.set_result(f.result())
            except Exception as e:
                done_future.set_exception(e)

    task.add_done_callback(_on_done)

    def _on_timeout():
        if not done_future.done():
            task.cancel(msg=f"Timeout after {timeout}s")  # 超时自动取消
            done_future.set_exception(TimeoutError())

    timeout_handle = loop.call_later(timeout, _on_timeout)

    # 如果任务提前完成，取消定时器
    def cancel_timeout(fut):
        if not timeout_handle.cancelled():
            timeout_handle.cancel()

    done_future.add_done_callback(cancel_timeout)

    return done_future


class AsyncSocket:
    def __init__(self, sock: socket.socket, loop=None):
        self._loop = loop or get_running_loop()
        self._sock = sock
        self._sock.setblocking(False)

    async def connect(self, addr: tuple):
        f = self._loop.create_future()

        try:
            self._sock.connect(addr)
            # 连接立即成功（如本地连接）
            f.set_result(None)
            return await f
        except (BlockingIOError, OSError) as e:
            # EINPROGRESS 表示连接正在进行中
            if e.errno not in (errno.EINPROGRESS, errno.EWOULDBLOCK):
                raise

        def _on_connected():
            self._loop.remove_writer(self._sock.fileno())
            err = self._sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if err == 0:
                f.set_result(None)
            else:
                f.set_exception(socket.error(err, os.strerror(err)))

        # 注册可写事件（连接完成时 socket 变为可写）
        self._loop.add_writer(self._sock.fileno(), _on_connected)
        return await f

    async def recv(self, n_bytes: int):
        f = self._loop.create_future()

        def _on_readable():
            try:
                data = self._sock.recv(n_bytes)
                self._loop.remove_reader(self._sock.fileno())
                f.set_result(data)
            except Exception as e:
                f.set_exception(e)

        self._loop.add_reader(self._sock.fileno(), _on_readable)
        return await f

    async def recv_all(self, chunk_size=4096):
        chunks = []
        while True:
            # 这次只注册一次可读事件
            f = self._loop.create_future()

            def _read_once():
                try:
                    part_data = self._sock.recv(chunk_size)
                except BlockingIOError:
                    # 还未就绪，重新等待（由回调机制保证）
                    return
                except Exception as e:
                    self._loop.remove_reader(self._sock.fileno())
                    f.set_exception(e)
                    return

                if not part_data:
                    # 连接关闭
                    self._loop.remove_reader(self._sock.fileno())
                    f.set_result(b"")
                else:
                    # 有数据，尝试继续读，直到没数据为止
                    chunks_local = [part_data]
                    while True:
                        try:
                            more = self._sock.recv(chunk_size)
                        except BlockingIOError:
                            break
                        except Exception as e:
                            self._loop.remove_reader(self._sock.fileno())
                            f.set_exception(e)
                            return
                        if not more:
                            self._loop.remove_reader(self._sock.fileno())
                            f.set_result(b"".join(chunks_local))
                            return
                        chunks_local.append(more)
                    self._loop.remove_reader(self._sock.fileno())
                    f.set_result(b"".join(chunks_local))

            self._loop.add_reader(self._sock.fileno(), _read_once)
            data = await f
            if not data:
                break
            chunks.append(data)
        return b"".join(chunks)

    async def sendall(self, data: bytes):
        f = self._loop.create_future()

        def _on_writable():
            try:
                self._sock.sendall(data)
                self._loop.remove_writer(self._sock.fileno())
                f.set_result(None)
            except Exception as e:
                f.set_exception(e)

        self._loop.add_writer(self._sock.fileno(), _on_writable)
        return await f

    def close(self):
        self._sock.close()
        # self._loop.remove_reader(self._sock.fileno())
        # self._loop.remove_writer(self._sock.fileno())


def main():
    def task1():
        print("task1 开始")
        yield sleep(1)
        print("task1 睡醒")
        return "done1"

    def task2():
        print("task2 开始")
        yield sleep(0.5)
        print("task2 睡醒")
        return "done2"

    def cancel_task(task: Task):
        print("取消任务")
        yield sleep(0.6)
        r = task.cancel()
        if not r:
            print("任务未取消")

    task1_task = current_loop.create_task(task1())
    task2_task = current_loop.create_task(task2())  # 同时运行
    cancel_task_task = current_loop.create_task(cancel_task(task1_task))

    # print((yield task1_task))
    # print((yield task2_task))
    all_done_future = gather(task1_task, task2_task, cancel_task_task)
    print((yield all_done_future))
    print("全部结束")
    return None


def main2():
    def test_task(task_id, delay):
        # print(f"task{task_id} 开始")
        yield sleep(delay)
        # print(f"task{task_id} 睡醒")
        return f"done{task_id}"

    tasks = [test_task(i, 0.1) for i in range(10000)]
    all_done_future = gather(*tasks)
    # print((yield all_done_future))
    yield all_done_future
    print("全部结束")
    return None


def main3():
    def test_task(task_id, delay):
        # print(f"task{task_id} 开始")
        yield sleep(delay)
        # print(f"task{task_id} 睡醒")
        return f"done{task_id}"

    res = wait_for(test_task(0, 1), 1.5)
    print((yield res))
    return None


async def main4():
    s = AsyncSocket(socket.socket())
    await s.connect(('www.baidu.com', 80))
    await s.sendall(b"GET / HTTP/1.1\r\nHost: www.baidu.com\r\nConnection: close\r\n\r\n")

    # 接收完整响应
    response = await s.recv_all()
    print(f"收到完整响应 ({len(response)} bytes)")

    # 分离头部和主体
    if b"\r\n\r\n" in response:
        headers, body = response.split(b"\r\n\r\n", 1)
        print("\n=== 响应头部 ===")
        print(headers.decode()[:500])
        print(f"\n=== 响应主体 ({len(body)} bytes) ===")
        print(body.decode()[:300])
    else:
        print(response.decode()[:500])

    s.close()


async def main5():
    """测试 Task name 和 cancel msg"""
    print("=" * 70)
    print("测试 Task name 和 cancel msg")
    print("=" * 70)

    loop = get_event_loop()

    # 创建带名称的任务
    async def long_task(name):
        print(f"[{name}] 开始执行...")
        try:
            await sleep(10)
            print(f"[{name}] 完成")
            return f"{name}_done"
        except CancelledError as e:
            print(f"[{name}] 被取消！")
            raise

    # 创建多个命名任务
    task1 = loop.create_task(long_task("Task-A"), name="HTTP-Request-1")
    task2 = loop.create_task(long_task("Task-B"), name="HTTP-Request-2")
    task3 = loop.create_task(long_task("Task-C"), name="Database-Query")

    print(f"\n任务列表:")
    print(f"  task1.name = {task1.name}")
    print(f"  task2.name = {task2.name}")
    print(f"  task3.name = {task3.name}")

    # 等待一会儿
    await sleep(0.5)

    # 取消 task2，带上原因
    print(f"\n取消 task2...")
    task2.cancel(msg="用户主动取消")

    # 再等一会儿
    await sleep(0.5)

    # 检查取消状态
    print(f"\ntask2 状态:")
    print(f"  cancelled: {task2.cancelled()}")
    print(f"  cancel_msg: {task2.cancel_msg()}")

    # 取消 task3，带上不同原因
    print(f"\n取消 task3...")
    task3.cancel(msg="超时限制")

    await sleep(0.5)

    print(f"\ntask3 状态:")
    print(f"  cancelled: {task3.cancelled()}")
    print(f"  cancel_msg: {task3.cancel_msg()}")

    # 清理 task1
    task1.cancel(msg="测试结束")

    print("\n✅ 测试完成")


async def main6():
    """测试 gather 失败时的 cancel msg"""
    print("\n" + "=" * 70)
    print("测试 gather 失败时的 cancel msg")
    print("=" * 70)

    async def success_task():
        print("[success] 开始")
        await sleep(0.5)
        print("[success] 完成")
        return "success"

    async def failing_task():
        print("[failing] 开始")
        await sleep(0.2)
        raise ValueError(" intentional error")

    try:
        results = await gather(
            success_task(),
            failing_task()
        )
        print(f"结果: {results}")
    except ValueError as e:
        print(f"\n✅ Gather 失败，捕获异常: {e}")
        print("   其他任务应该已被取消并带有 cancel msg")


def main7():
    import asyncio

    async def asyncio_test():
        async def test_task(task_id, delay):
            await asyncio.sleep(delay)
            return f"done{task_id}"

        tasks = [test_task(i, 0.1) for i in range(10000)]
        results = await asyncio.gather(*tasks)

    asyncio.run(asyncio_test())


if __name__ == "__main__":
    start_time = time.time()
    run(main2())  # 框架测试
    print("总耗时：", time.time() - start_time)

    # run(main4())  # HTTP 请求测试
    # run(main5())  # Task name 和 cancel msg 测试
    # run(main6())  # gather cancel msg 测试
    start_time = time.time()
    main7()  # asyncio 测试
    print("总耗时：", time.time() - start_time)
