#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
微型异步事件循环框架实现

提供了一个简化版的 asyncio 实现，包含：
- 事件循环（EventLoop）
- Future/Task 系统
- 定时器管理（TimerHandle）
- I/O 多路复用（基于 selectors）
- 线程安全调度（call_soon_threadsafe）
- 异步原语（sleep, gather, wait, wait_for）
- 异步 Socket 封装（AsyncSocket）

支持生成器协程和原生协程（async/await）。

Author: Ljw(dsdcyy)
Created: 2026/5/4
"""
import contextvars
import errno
import heapq
import os
import selectors
import socket
import threading
import time
import weakref
from collections import deque
from concurrent.futures import ThreadPoolExecutor, Future as ThreadFuture
from contextvars import ContextVar
from types import GeneratorType, CoroutineType
from typing import (
    Generator,
    Awaitable,
    Any,
    Callable,
    Optional,
    Tuple,
    Set,
    List,
    Union,
)

# 使用 ContextVar 存储当前事件循环（线程安全、协程安全）
_loop_var: ContextVar[Optional["EventLoop"]] = ContextVar("_loop", default=None)

FIRST_COMPLETED = "FIRST_COMPLETED"
FIRST_EXCEPTION = "FIRST_EXCEPTION"
ALL_COMPLETED = "ALL_COMPLETED"


def get_running_loop() -> "EventLoop":
    """
    获取当前正在运行的事件循环

    Returns:
        EventLoop: 当前运行的事件循环实例

    Raises:
        RuntimeError: 如果没有正在运行的事件循环
    """
    loop = _loop_var.get()
    if loop is None:
        raise RuntimeError("No running event loop")
    return loop


def get_event_loop() -> "EventLoop":
    """
    获取或创建事件循环

    Returns:
        EventLoop: 事件循环实例（如果不存在则创建新的）

    Note:
        此函数会复用已存在的事件循环。
        如果需要新的事件循环，请直接创建 EventLoop() 实例。
    """
    loop = _loop_var.get()
    if loop is None:
        loop = EventLoop()
        _loop_var.set(loop)
    return loop


class CancelledError(Exception):
    """任务或 Future 被取消时抛出的异常"""

    pass


class TimeoutError(Exception):
    """操作超时时抛出的异常"""

    pass


class YieldControl:
    """
    用于同步生成器让出控制权的标记对象（单例）

    用法:
        def sync_task():
            # 做一些工作
            yield yield_control()  # 让出控制权
            # 继续工作
    """

    _instance = None

    def __new__(cls) -> "YieldControl":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance


def yield_control() -> YieldControl:
    """
    在同步生成器中调用，让出控制权给其他任务

    Returns:
        YieldControl: 单例标记对象

    Example:
        >>> def cpu_heavy_task():
        ...     for i in range(100):
        ...         result = sum(j * j for j in range(1000))
        ...         yield yield_control()  # 让出控制权
        ...     return result
    """
    return YieldControl()


class Future:
    """
    异步操作的结果占位符

    Future 代表一个尚未完成的异步操作的结果。
    可以设置结果、异常，或取消操作。

    Attributes:
        _result: 操作结果
        _exception: 操作异常
        _done: 是否已完成
        _cancelled: 是否被取消
        _cancel_msg: 取消消息
        _callbacks: 完成回调列表
        _loop: 关联的事件循环
    """

    __slots__ = (
        "_result",
        "_exception",
        "_done",
        "_cancelled",
        "_cancel_msg",
        "_callbacks",
        "_loop",
    )

    def __init__(self, loop: Optional["EventLoop"] = None) -> None:
        """
        初始化 Future

        Args:
            loop: 关联的事件循环（默认为当前运行的循环）
        """
        self._result = None
        self._exception = None
        self._done = False
        self._cancelled = False
        self._cancel_msg = None
        self._callbacks: List[Callable[["Future"], Any]] = []
        self._loop = loop or get_running_loop()

    def _check_done(self, check_true: bool = True) -> None:
        """
        检查 Future 的完成状态

        Args:
            check_true: 如果为 True，检查是否已完成；否则检查是否未完成

        Raises:
            RuntimeError: 状态不符合预期
        """
        if self._done is check_true:
            if check_true:
                raise RuntimeError("Future already done")
            raise RuntimeError("Future is not done")

    def _finish(self, result: Any = None, exc: Optional[Exception] = None) -> None:
        """
        完成 Future，设置结果或异常，并触发回调

        Args:
            result: 操作结果
            exc: 操作异常
        """
        self._check_done()
        self._result = result
        self._exception = exc
        self._done = True
        callbacks = self._callbacks
        self._callbacks = []
        for cb in callbacks:
            self._loop.call_soon(cb, self)

    def set_result(self, result: Any) -> None:
        """
        设置 Future 的结果

        Args:
            result: 操作结果

        Raises:
            RuntimeError: 如果 Future 已经完成
        """
        self._finish(result)

    def set_exception(self, exc: Exception) -> None:
        """
        设置 Future 的异常

        Args:
            exc: 异常对象

        Raises:
            RuntimeError: 如果 Future 已经完成
        """
        self._finish(exc=exc)

    def add_done_callback(self, cb: Callable[["Future"], Any]) -> None:
        """
        添加完成回调

        Args:
            cb: 回调函数，接收 Future 作为参数
        """
        if self._done:
            self._loop.call_soon(cb, self)
        else:
            self._callbacks.append(cb)

    def done(self) -> bool:
        """
        检查 Future 是否已完成

        Returns:
            bool: 如果已完成返回 True
        """
        return self._done

    def exception(self) -> Optional[Exception]:
        """
        获取 Future 的异常

        Returns:
            Exception or None: 异常对象，如果没有异常则为 None

        Raises:
            RuntimeError: 如果 Future 还未完成
        """
        self._check_done(False)
        return self._exception

    def result(self) -> Any:
        """
        获取 Future 的结果

        Returns:
            Any: 操作结果

        Raises:
            RuntimeError: 如果 Future 还未完成
            Exception: 如果 Future 有异常，则抛出该异常
        """
        self._check_done(False)
        if self._exception is not None:
            raise self._exception
        return self._result

    def __iter__(self) -> Generator["Future", Any, Any]:
        """
        让 Future 可被 yield，生成器通过 yield future 来等待

        Returns:
            Generator: 生成器对象
        """
        result = yield self
        return result

    def __await__(self) -> Generator["Future", Any, Any]:
        """
        让 Future 可被 await，支持原生协程

        Returns:
            Generator: 生成器对象
        """
        # __await__ 必须返回一个迭代器
        return self.__iter__()

    def cancel(self, msg: Optional[str] = None) -> bool:
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

    def cancelled(self) -> bool:
        """
        检查 Future 是否被取消

        Returns:
            bool: 如果被取消返回 True
        """
        return self._cancelled

    def cancel_msg(self) -> Optional[str]:
        """
        获取取消时的消息

        Returns:
            str or None: 取消消息
        """
        return self._cancel_msg


class Task(Future):
    """
    异步任务，将生成器或协程包装成 Future

    Task 继承自 Future，负责驱动生成器/协程一步步执行。
    它管理任务的调度、取消和完成状态。

    Attributes:
        gen: 被包装的生成器或协程对象
        name: 任务名称（用于调试和日志）
        _next_value: 下一次发送给生成器的值
        _stepping: 是否正在执行 _step（防止重入）
        _rescheduled: 是否已重新调度（处理重入情况）
        _waiting: 当前等待的 Future 对象
    """

    __slots__ = (
        "gen",
        "name",
        "_next_value",
        "_stepping",
        "_rescheduled",
        "_waiting",
        "_context",
    )

    def __init__(
        self,
        gen: Union[Awaitable[Any], Generator[Any, Any, Any]],
        loop: Optional["EventLoop"] = None,
        name: Optional[str] = None,
    ) -> None:
        """
        初始化 Task

        Args:
            gen: 生成器或协程对象
            loop: 关联的事件循环（默认为当前运行的循环）
            name: 任务名称（可选，未提供则自动生成）
        """
        super().__init__(loop)
        self.gen = gen
        self.name = name or f"Task-{id(self)}"  # 任务名称
        # 启动生成器，并处理它遇到的第一个 yield
        self._loop.call_soon(self._step)
        self._next_value: Any = None
        self._stepping = False
        self._rescheduled = False
        self._waiting: Optional[Future] = None  # 当前等待的 Future
        self._context = contextvars.copy_context()

    def _on_done(self, f: Future) -> None:
        """
        当等待的 Future 完成时的回调

        Args:
            f: 已完成的 Future 对象
        """
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

    def cancelled(self) -> bool:
        """
        检查 Task 是否被取消（基于异常类型判断）

        Returns:
            bool: 如果任务被取消返回 True

        Note:
            与 Future.cancelled() 不同，这里基于异常类型判断，
            因为 Task 在取消处理过程中会重置 _cancelled 标志。
        """
        return self._done and isinstance(self._exception, CancelledError)

    def cancel(self, msg: Optional[str] = None) -> bool:
        """
        取消任务

        Args:
            msg: 取消原因消息（可选）

        Returns:
            bool: 是否成功取消

        Note:
            取消时会传播到当前等待的 Future（如果有），
            并调度 _step 以注入 CancelledError。
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

    def _step(self, value: Any = None, is_exc: bool = False) -> None:
        """
        在上下文运行 _real_step 方法
        """
        self._context.run(self._real_step, value, is_exc)

    def _real_step(self, value: Any = None, is_exc: bool = False) -> None:
        """
        驱动生成器执行一步

        这是 Task 的核心方法，负责：
        1. 向生成器发送值或抛出异常
        2. 处理生成器的 yield 结果
        3. 管理任务的完成状态

        Args:
            value: 发送给生成器的值，或要抛出的异常
            is_exc: 如果为 True，value 是异常对象，将抛出；否则作为值发送

        Workflow:
            1. 检查重入保护（_stepping）
            2. 根据 is_exc 决定 send() 或 throw()
            3. 处理 StopIteration（任务完成）
            4. 处理其他异常（任务失败）
            5. 处理 yield 的结果：
               - Future: 等待完成后继续
               - Generator/Coroutine: 包装成 Task 后等待
               - YieldControl: 让出控制权
               - 其他值: 作为下一次 send 的输入
        """
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
    """
    定时器句柄，代表一个已排期的延迟回调

    TimerHandle 由 EventLoop.call_later() 创建，
    允许用户取消尚未执行的定时器。

    Attributes:
        _callback: 到期时执行的回调函数
        _args: 回调函数的参数
        _loop_ref: 事件循环的弱引用（避免循环引用）
        _when: 定时器的到期时间（单调时间）
        _cancelled: 是否已被取消
    """

    __slots__ = ("_callback", "_args", "_loop_ref", "_when", "_cancelled")

    def __init__(
        self,
        callback: Callable[..., Any],
        args: Tuple[Any, ...],
        loop: "EventLoop",
        when: float,
    ) -> None:
        """
        初始化定时器句柄

        Args:
            callback: 到期时执行的回调函数
            args: 回调函数的参数元组
            loop: 关联的事件循环
            when: 到期时间（单调时间戳）
        """
        self._callback = callback
        self._args = args
        self._loop_ref = weakref.ref(loop)  # 弱引用

        self._when = when  # 记录到期时间，便于调试
        self._cancelled = False

    def cancel(self) -> None:
        """
        取消定时器

        取消后，定时器到期时其回调将不会执行。
        这是一个幂等操作，可以安全地多次调用。
        """
        self._cancelled = True

    def run(self) -> None:
        """
        执行定时器回调

        如果定时器未被取消，则将回调排入事件循环的就绪队列。
        此方法由事件循环在定时器到期时自动调用。
        """
        if not self._cancelled:
            loop = self._loop_ref()
            if loop:
                loop.call_soon(self._callback, *self._args)

    def cancelled(self) -> bool:
        """
        检查定时器是否已被取消

        Returns:
            bool: 如果已取消返回 True
        """
        return self._cancelled

    def __repr__(self) -> str:
        """
        返回定时器的字符串表示

        Returns:
            str: 格式为 <TimerHandle when=X.XXX status>
        """
        status = "cancelled" if self._cancelled else "scheduled"
        return f"<TimerHandle when={self._when:.3f} {status}>"

    def when(self) -> float:
        """
        返回定时器的到期时间

        Returns:
            float: 到期时间（单调时间戳）
        """
        return self._when


class EventLoop:
    """
    异步事件循环核心实现

    EventLoop 是整个异步框架的核心，负责：
    - 调度和管理异步任务
    - 处理 I/O 多路复用（基于 selectors）
    - 管理定时器
    - 提供线程安全的回调调度
    - 执行异步原语（sleep, gather, wait 等）

    Attributes:
        _ready: 就绪回调队列（deque）
        _scheduled: 定时器堆（最小堆）
        _timer_seq: 定时器序列号（用于堆排序稳定性）
        _running: 是否正在运行
        _tasks: 所有活跃任务的集合
        _selector: I/O 多路复用器
        _exception_handler: 自定义异常处理器
        _execute_pool: 线程池执行器
        _ready_lock: 保护 _ready 的线程锁
        _closed: 是否已关闭
        _self_reader: 自唤醒管道读端
        _self_writer: 自唤醒管道写端
    """

    def __init__(self) -> None:
        """
        初始化事件循环

        创建必要的数据结构和自唤醒管道。
        """
        self._ready: deque = deque()  # 就绪回调队列
        self._scheduled: list = []  # 最小堆，存 (deadline, seq, handler)
        self._timer_seq = 0
        self._running = False
        self._tasks: Set[Task] = set()
        self._selector = selectors.DefaultSelector()
        self._exception_handler: Optional[Callable[["EventLoop", dict], None]] = None
        self._execute_pool: Optional[ThreadPoolExecutor] = None
        self._ready_lock = threading.Lock()  # 保护 _ready 的线程锁
        self._closed = False  # 标记是否已关闭

        # 创建自唤醒管道
        self._self_reader, self._self_writer = socket.socketpair()
        self._self_reader.setblocking(False)
        self._self_writer.setblocking(False)

        # 使用统一的数据结构存储 read/write 回调列表，便于支持持久/一次性回调
        self._selector.register(
            self._self_reader.fileno(),
            selectors.EVENT_READ,
            {"read": [(self._consume_wake, (), False)], "write": []},
        )

    def is_closed(self) -> bool:
        """
        检查事件循环是否已关闭

        Returns:
            bool: 如果已关闭返回 True
        """
        return self._closed

    def close(self) -> None:
        """
        关闭事件循环，释放资源

        清理自唤醒管道、selector 和线程池。
        这是一个幂等操作，可以安全地多次调用。
        """
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

    def _consume_wake(self) -> None:
        """
        读取自唤醒管道中的字节（消费唤醒信号）

        当其他线程调用 call_soon_threadsafe 时，
        会向写端发送数据，触发此回调以打断 selector.select() 的阻塞。
        """
        try:
            while True:
                self._self_reader.recv(4096)
        except BlockingIOError:
            pass  # 读空为止
        except OSError:
            pass

    def call_soon_threadsafe(self, callback: Callable[..., Any], *args: Any) -> None:
        """
        线程安全版本的 call_soon，供其他线程调用

        从其他线程向事件循环提交回调。使用锁保护 _ready 队列，
        并通过自唤醒管道打断 selector.select() 的阻塞。

        Args:
            callback: 要执行的回调函数
            *args: 回调函数的参数

        Thread Safety:
            此方法是线程安全的，可以从任何线程调用。
        """
        with self._ready_lock:
            self.call_soon(callback, *args)
        self._wake_loop()

    def _wake_loop(self) -> None:
        """
        向自唤醒管道写入一个字节，打断 selector.select()

        当有其他线程通过 call_soon_threadsafe 添加回调时，
        需要唤醒事件循环以立即处理新任务。
        """
        try:
            self._self_writer.send(b"\x00")
        except OSError:
            pass  # 写端已关闭等异常忽略

    def run_in_thread_executor(
        self, func: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> Future:
        """
        在线程池中异步执行同步函数，返回 Future

        将阻塞的同步函数放到线程池中执行，不阻塞事件循环。
        支持取消：如果 Future 被取消，也会尝试取消线程任务。

        Args:
            func: 要执行的同步函数
            *args: 位置参数
            **kwargs: 关键字参数

        Returns:
            Future: 代表异步操作结果的 Future 对象

        Example:
            >>> def blocking_io():
            ...     time.sleep(5)
            ...     return "done"
            >>> result = await loop.run_in_thread_executor(blocking_io)
        """
        if self._execute_pool is None:
            self._execute_pool = ThreadPoolExecutor()

        tf = self._execute_pool.submit(func, *args, **kwargs)
        f = self.create_future()

        # 支持取消：当 Future 被取消时，也取消线程任务
        def cancel_thread_future(fut: Future):
            if fut.cancelled() and not tf.done():
                tf.cancel()

        f.add_done_callback(cancel_thread_future)

        def _on_thread_done(t_fut: ThreadFuture):
            def set_result():
                try:
                    res = t_fut.result()
                except Exception as e:
                    f.set_exception(e)
                else:
                    f.set_result(res)

            self.call_soon_threadsafe(set_result)

        tf.add_done_callback(_on_thread_done)
        return f

    def set_exception_handler(
        self, handler: Optional[Callable[["EventLoop", dict], None]]
    ) -> None:
        """
        设置自定义异常处理器

        Args:
            handler: 异常处理函数，接收 (loop, context) 参数。
                    如果为 None，则使用默认处理器（打印到 stdout）。

        Raises:
            TypeError: 如果 handler 不是可调用对象或 None
        """
        if handler is not None and not callable(handler):
            raise TypeError("handler must be callable or None")
        self._exception_handler = handler

    def call_exception_handler(self, context: dict) -> None:
        """
        调用异常处理器

        Args:
            context: 异常上下文字典，包含 'message', 'exception' 等键
        """
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
            exc = context.get("exception")
            if exc:
                import traceback

                traceback.print_exception(type(exc), exc, exc.__traceback__)

    def call_soon(self, callback: Callable[..., Any], *args: Any) -> None:
        """
        立即安排回调执行

        将回调添加到就绪队列，在下一个事件循环迭代中执行。

        Args:
            callback: 要执行的回调函数
            *args: 回调函数的参数

        Note:
            此方法不是线程安全的。从其他线程调用应使用 call_soon_threadsafe。
        """
        self._ready.append((callback, args))

    def call_later(
        self, delay: float, callback: Callable[..., Any], *args: Any
    ) -> TimerHandle:
        """
        延迟执行回调

        创建一个定时器，在 delay 秒后执行回调。

        Args:
            delay: 延迟时间（秒）
            callback: 到期时执行的回调函数
            *args: 回调函数的参数

        Returns:
            TimerHandle: 定时器句柄，可用于取消定时器

        Example:
            >>> handle = loop.call_later(1.0, print, "Hello")
            >>> handle.cancel()  # 取消定时器
        """
        deadline = time.monotonic() + delay
        self._timer_seq += 1
        handler = TimerHandle(callback, args, self, deadline)
        heapq.heappush(self._scheduled, (deadline, self._timer_seq, handler))
        return handler

    def add_reader(
        self,
        fileobj: int,
        callback: Callable[..., Any],
        *args: Any,
        one_shot: bool = False,
    ) -> None:
        """
        注册文件描述符的可读事件

        Args:
            fileobj: 文件描述符或 socket
            callback: 可读时执行的回调
            *args: 回调参数
            one_shot: 是否仅执行一次（默认 False）
        """
        self._add_io_callback("read", fileobj, callback, args, one_shot)

    def add_writer(
        self,
        fileobj: int,
        callback: Callable[..., Any],
        *args: Any,
        one_shot: bool = False,
    ) -> None:
        """
        注册文件描述符的可写事件

        Args:
            fileobj: 文件描述符或 socket
            callback: 可写时执行的回调
            *args: 回调参数
            one_shot: 是否仅执行一次（默认 False）
        """
        self._add_io_callback("write", fileobj, callback, args, one_shot)

    def remove_reader(self, fileobj: int) -> None:
        """
        移除文件描述符的可读事件

        Args:
            fileobj: 文件描述符或 socket
        """
        self._remove_io_callbacks("read", fileobj)

    def remove_writer(self, fileobj: int) -> None:
        """
        移除文件描述符的可写事件

        Args:
            fileobj: 文件描述符或 socket
        """
        self._remove_io_callbacks("write", fileobj)

    def _fd(self, fileobj: int) -> int:
        """返回文件描述符整数。"""
        return fileobj if isinstance(fileobj, int) else fileobj.fileno()

    def _add_io_callback(
        self,
        direction: str,
        fileobj: int,
        callback: Callable[..., Any],
        args: Tuple[Any, ...],
        one_shot: bool,
    ) -> None:
        """内部：向 selector 的 direction 列表添加回调并更新注册。"""
        fd = self._fd(fileobj)
        event = selectors.EVENT_READ if direction == "read" else selectors.EVENT_WRITE
        try:
            key = self._selector.get_key(fd)
        except KeyError:
            data = {"read": [], "write": []}
            data[direction].append((callback, args, one_shot))
            self._selector.register(fd, event, data)
        else:
            data = key.data
            data.setdefault(direction, []).append((callback, args, one_shot))
            events = key.events | event
            self._selector.modify(fd, events, data)

    def _update_selector_registration(self, fd: int, data: dict) -> None:
        """内部：根据 data['read']/['write'] 的内容，修改或注销 selector 的注册。

        如果两个方向都没有回调则注销，否则根据存在的方向设置事件掩码。
        在高并发场景中可能遇到 KeyError，统一忽略该异常。
        """
        new_events = 0
        if data.get("read"):
            new_events |= selectors.EVENT_READ
        if data.get("write"):
            new_events |= selectors.EVENT_WRITE

        try:
            if new_events:
                self._selector.modify(fd, new_events, data)
            else:
                try:
                    self._selector.unregister(fd)
                except KeyError:
                    pass
        except KeyError:
            # 在高并发下，fd 可能已被移除，忽略即可
            pass

    def _remove_io_callbacks(self, direction: str, fileobj: int) -> None:
        """内部：清空 direction 的回调并根据剩余回调修改或注销 selector 注册。"""
        fd = self._fd(fileobj)
        try:
            key = self._selector.get_key(fd)
        except KeyError:
            return
        data = key.data
        data[direction] = []
        self._update_selector_registration(fd, data)

    def _remove_specific_io_callback(
        self, direction: str, fileobj: int, callback: Callable[..., Any]
    ) -> None:
        """内部：从 direction 列表中移除指定回调（按函数对象匹配）。"""
        fd = self._fd(fileobj)
        try:
            key = self._selector.get_key(fd)
        except KeyError:
            return
        data = key.data
        lst = data.get(direction, [])
        # 移除所有与 callback 相同的回调条目
        data[direction] = [item for item in lst if item[0] is not callback]
        self._update_selector_registration(fd, data)

    def add_reader_threadsafe(
        self,
        fileobj: int,
        callback: Callable[..., Any],
        *args: Any,
        one_shot: bool = False,
    ) -> None:
        """线程安全地注册可读回调（可从其他线程调用）。"""
        # 使用 call_soon_threadsafe 在事件循环线程中执行实际注册
        self.call_soon_threadsafe(
            self._add_io_callback, "read", fileobj, callback, args, one_shot
        )

    def add_writer_threadsafe(
        self,
        fileobj: int,
        callback: Callable[..., Any],
        *args: Any,
        one_shot: bool = False,
    ) -> None:
        """线程安全地注册可写回调（可从其他线程调用）。"""
        self.call_soon_threadsafe(
            self._add_io_callback, "write", fileobj, callback, args, one_shot
        )

    def remove_reader_callback(
        self, fileobj: int, callback: Callable[..., Any]
    ) -> None:
        """从 `fileobj` 的可读回调列表中移除指定回调（非线程安全）。"""
        self._remove_specific_io_callback("read", fileobj, callback)

    def remove_reader_callback_threadsafe(
        self, fileobj: int, callback: Callable[..., Any]
    ) -> None:
        """线程安全地从 `fileobj` 的可读回调列表中移除指定回调。"""
        self.call_soon_threadsafe(
            self._remove_specific_io_callback, "read", fileobj, callback
        )

    def remove_writer_callback(
        self, fileobj: int, callback: Callable[..., Any]
    ) -> None:
        """从 `fileobj` 的可写回调列表中移除指定回调（非线程安全）。"""
        self._remove_specific_io_callback("write", fileobj, callback)

    def remove_writer_callback_threadsafe(
        self, fileobj: int, callback: Callable[..., Any]
    ) -> None:
        """线程安全地从 `fileobj` 的可写回调列表中移除指定回调。"""
        self.call_soon_threadsafe(
            self._remove_specific_io_callback, "write", fileobj, callback
        )

    def create_task(
        self,
        gen: Union[Generator[Future, Any, Any], Awaitable[Any]],
        name: Optional[str] = None,
    ) -> Task:
        """
        创建异步任务

        将生成器或协程包装成 Task，并添加到事件循环的任务集合中。

        Args:
            gen: 生成器或协程对象
            name: 任务名称（可选，用于调试和日志）

        Returns:
            Task: 创建的任务对象

        Example:
            >>> async def my_coro():
            ...     await sleep(1)
            ...     return "done"
            >>> task = loop.create_task(my_coro(), name="MyTask")
        """
        task = Task(gen, self, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._task_done)
        return task

    def _task_done(self, task: Task) -> None:
        """
        任务完成时的回调

        从任务集合中移除已完成的任务。

        Args:
            task: 已完成的任务
        """
        self._tasks.discard(task)

    def create_future(self) -> Future:
        """
        创建一个新的 Future 对象

        Returns:
            Future: 与当前事件循环关联的 Future 对象
        """
        return Future(self)

    def gather(
        self, *coro_s: Union[Awaitable[Any], Generator[Future, Any, Any], Task]
    ) -> Future:
        """
        便捷方法：调用模块级 `gather`。
        """
        return gather(*coro_s)

    def wait(
        self,
        fs: List[Union[Awaitable[Any], Generator[Future, Any, Any], Task]],
        timeout: Optional[float] = None,
        return_when: str = ALL_COMPLETED,
    ) -> Future:
        """
        便捷方法：调用模块级 `wait`。
        """
        return wait(fs, timeout=timeout, return_when=return_when)

    def wait_for(
        self, aw: Union[Awaitable[Any], Generator[Future, Any, Any]], timeout: float
    ) -> Future:
        """
        便捷方法：调用模块级 `wait_for`。
        """
        return wait_for(aw, timeout=timeout)

    def run(self, awaitable: Union[Generator[Future, Any, Any], Task, Future]) -> Any:
        """
        直接运行一个 awaitable（生成器/Task/Future）并返回最终结果。

        便捷入口，等价于：
            task = loop.create_task(coro_gen)
            loop.run_until_complete(task)
            return task.result()

        Note:
            此方法会自动设置 ContextVar，使 get_running_loop() 可用。
        """
        token = _loop_var.set(self)
        try:
            # if isinstance(awaitable, Task):
            #     task = awaitable
            # elif isinstance(awaitable, Future):
            #     task = awaitable
            # else:
            #     # 在 ContextVar 设置后创建 Task，确保捕获正确的 Context
            #     task = self.create_task(awaitable)
            task = self.ensure_task(awaitable)
            self.run_until_complete(task)
            return task.result()
        finally:
            _loop_var.reset(token)

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
                "message": "Exception in callback",
                "exception": e,
                "callback": cb,
                "args": args,
            }
            self.call_exception_handler(context)

    def _dispatch_selector_event(self, key, mask) -> None:
        """
        将 selector 事件的分发逻辑提取出来，统一处理 read/write 列表中的回调。

        参数:
            key: selector 返回的 key 对象
            mask: 事件掩码

        行为:
            - 对 read/write 两个方向分别处理回调列表
            - 调用回调时使用 `call_soon`
            - 移除标记为一次性（one_shot）的回调
            - 根据剩余回调更新 selector 的注册（modify/unregister）
        """
        data = key.data
        fd = key.fileobj

        for direction, event_flag in (
            ("read", selectors.EVENT_READ),
            ("write", selectors.EVENT_WRITE),
        ):
            if not (mask & event_flag):
                continue

            # 复制列表以便在回调过程中安全遍历/修改原始列表
            callback_list = list(data.get(direction, []))
            for item in callback_list:
                cb, args, one_shot = item
                try:
                    self.call_soon(cb, *args)
                except Exception:
                    # call_soon 本身不应抛，但捕获以防止影响其它回调
                    context = {
                        "message": "Exception scheduling callback",
                        "exception": None,
                        "callback": cb,
                        "args": args,
                    }
                    self.call_exception_handler(context)

                if one_shot:
                    try:
                        data[direction].remove(item)
                    except ValueError:
                        pass

        # 更新 selector 注册（封装以避免重复代码）
        self._update_selector_registration(fd, data)

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
                # 将事件处理委托到单独方法，减少重复并提高可测性
                try:
                    self._dispatch_selector_event(key, mask)
                except Exception as e:
                    context = {
                        "message": "Exception while dispatching selector event",
                        "exception": e,
                        "key": key,
                        "mask": mask,
                    }
                    self.call_exception_handler(context)
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

    def ensure_task(
        self, task: Union[Awaitable[Any], Generator[Future, Any, Any], Task, Future]
    ) -> Task:
        """
        确保输入被转换为 Task 对象

        将支持的输入类型统一转换为 Task：
        - Task: 直接返回
        - Future: 包装成 Task
        - 协程/生成器: 创建新的 Task

        Args:
            task: 要转换的对象，支持 Task、Future、协程、生成器

        Returns:
            Task: 转换后的 Task 对象

        Raises:
            TypeError: 如果输入类型不支持

        Example:
            >>> async def my_coro():
            ...     return "done"
            >>> task = loop.ensure_task(my_coro())
            >>> isinstance(task, Task)  # True
        """
        if isinstance(task, Task):
            # 已经是 Task，直接返回
            return task
        elif isinstance(task, Future):
            # Future 需要包装成 Task
            # 创建一个空的 Task 来包装这个 Future
            async def wrapper():
                return await task

            new_task = self.create_task(wrapper())
            return new_task
        elif isinstance(task, (GeneratorType, CoroutineType)) or hasattr(
            task, "__await__"
        ):
            # 协程或生成器，创建新的 Task
            return self.create_task(task)
        else:
            raise TypeError(
                f"Unsupported type: {type(task).__name__}. "
                f"Expected Task, Future, coroutine, or generator."
            )


def sleep(delay: float) -> Future:
    """
    异步睡眠

    创建一个 Future，在 delay 秒后完成。
    支持取消：如果 Future 被取消，定时器也会被取消。

    Args:
        delay: 睡眠时间（秒）

    Returns:
        Future: 代表睡眠操作的 Future

    Example:
        >>> await sleep(1.0)  # 睡眠 1 秒
    """
    loop = get_running_loop()
    f = loop.create_future()
    handle = loop.call_later(delay, f.set_result, None)

    def on_cancel(fut: Future) -> None:
        if fut.cancelled() and not handle.cancelled():
            handle.cancel()

    f.add_done_callback(on_cancel)
    return f


def run(awaitable: Union[Awaitable[Any], Generator[Future, Any, Any]]) -> Any:
    """
    运行异步代码的入口点

    类似 asyncio.run()，创建一个新的事件循环，运行 awaitable，
    然后自动关闭并清理事件循环。

    Args:
        awaitable: 要运行的协程、生成器或 Future

    Returns:
        Any: awaitable 的返回值

    Raises:
        RuntimeError: 如果已经有事件循环在运行（禁止嵌套调用）

    Example:
        >>> async def main():
        ...     await sleep(1)
        ...     return "done"
        >>> result = run(main())
    """
    # 检查是否已有事件循环在运行
    if _loop_var.get() is not None:
        raise RuntimeError("Cannot run the event loop while another is running")

    loop = EventLoop()

    # 使用 ContextVar 设置当前事件循环（自动恢复）
    token = _loop_var.set(loop)
    try:
        return loop.run(awaitable)
    finally:
        loop.close()
        _loop_var.reset(token)  # 恢复之前的上下文


def gather(*coro_s: Union[Awaitable[Any], Generator[Future, Any, Any], Task]) -> Future:
    """
    并发运行多个协程/任务，等待它们全部完成

    类似 asyncio.gather()，并发执行多个异步操作，
    并返回一个包含所有结果的列表（按输入顺序）。
    如果任何一个任务失败，会取消其他所有任务。

    Args:
        *coro_s: 要并发执行的协程、生成器或 Task

    Returns:
        Future: 结果是按顺序排列的所有返回值列表

    Raises:
        Exception: 如果任何一个任务抛出异常

    Example:
        >>> async def task1():
        ...     await sleep(0.1)
        ...     return "result1"
        >>> async def task2():
        ...     await sleep(0.2)
        ...     return "result2"
        >>> results = await gather(task1(), task2())
        >>> print(results)  # ["result1", "result2"]
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
    task_to_index = {}

    def _on_task_done(task_f: Future):
        nonlocal completed_count, failed

        if failed:  # 如果已经有一个任务失败了，忽略其他的
            return

        idx = task_to_index.pop(task_f, None)
        if idx is None:
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

    def _propagate_cancel(f: Future):
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
        # 绑定共享回调，避免为每个任务创建 lambda 闭包。
        task_to_index[task] = i
        task.add_done_callback(_on_task_done)
        tasks.append(task)
    all_done_future.add_done_callback(_propagate_cancel)
    return all_done_future


def wait(
    fs: List[Union[Awaitable[Any], Generator[Future, Any, Any], Task]],
    timeout: Optional[float] = None,
    return_when: str = ALL_COMPLETED,
) -> Future:
    """
    等待一组任务完成

    类似 asyncio.wait()，等待多个异步操作完成，
    并根据 return_when 参数决定何时返回。

    Args:
        fs: 要等待的协程、生成器或 Task 列表
        timeout: 超时时间（秒），None 表示不超时
        return_when: 返回条件：
            - FIRST_COMPLETED: 第一个任务完成时返回
            - FIRST_EXCEPTION: 第一个任务异常或全部完成时返回
            - ALL_COMPLETED: 所有任务完成时返回（默认）

    Returns:
        Future: 结果是 (done_set, pending_set) 元组
            - done_set: 已完成的任务集合
            - pending_set: 未完成的任务集合

    Example:
        >>> done, pending = await wait([task1, task2], timeout=5.0)
        >>> for task in done:
        ...     print(task.result())
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


def wait_for(
    aw: Union[Awaitable[Any], Generator[Future, Any, Any]], timeout: float
) -> Future:
    """
    等待单个异步操作完成，带超时限制

    类似 asyncio.wait_for()，如果操作在 timeout 秒内未完成，
    则取消该操作并抛出 TimeoutError。

    Args:
        aw: 要等待的协程、生成器或 Task
        timeout: 超时时间（秒）

    Returns:
        Future: 异步操作的结果

    Raises:
        TimeoutError: 如果操作超时

    Example:
        >>> async def slow_task():
        ...     await sleep(10)
        ...     return "done"
        >>> try:
        ...     result = await wait_for(slow_task(), timeout=1.0)
        ... except TimeoutError:
        ...     print("Task timed out")
    """
    loop = get_running_loop()
    done_future = loop.create_future()

    # 包装成 Task（支持生成器协程和原生协程）
    task = loop.ensure_task(aw)

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
    def cancel_timeout(fut: Future):
        if fut.done() and not timeout_handle.cancelled():
            timeout_handle.cancel()

    done_future.add_done_callback(cancel_timeout)

    return done_future


def create_task(
    aw: Union[Awaitable[Any], Generator[Future, Any, Any]], name: str = None
) -> Task:
    """
    创建一个 asyncio.Task 对象

    Args:
        aw: 要包装的协程、生成器或 Future
        name: 任务的名称，用于调试和日志记录
    Returns:
        Task: 包装后的 Task 对象
    """
    loop = get_running_loop()
    return loop.create_task(aw, name=name)


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
