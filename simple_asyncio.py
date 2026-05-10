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
import logging
import os
import selectors
import socket
import threading
import time
import weakref
from collections import deque
from concurrent.futures import ThreadPoolExecutor, Future as ThreadFuture
from contextlib import contextmanager
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
    TypeVar,
    Generic,
    Literal,
    AsyncGenerator,
)

__all__ = (
    "ALL_COMPLETED",
    "AsyncCountdownLock",
    "AsyncLock",
    "AsyncQueue",
    "AsyncSelectiveLock",
    "AsyncSemaphore",
    "AsyncSocket",
    "AsyncTimeoutError",
    "AsyncToggleLock",
    "BaseAsyncLock",
    "Event",
    "EventLoop",
    "FIRST_COMPLETED",
    "FIRST_EXCEPTION",
    "FutureCancelledError",
    "QueueEmptyError",
    "QueueFullError",
    "SelectiveLockBase",
    "Task",
    "ThreadFuture",
    "TimerHandle",
    "WaiterEntry",
    "YieldControl",
    "create_task",
    "gather",
    "get_event_loop",
    "get_running_loop",
    "get_running_loop_safe",
    "logger",
    "run",
    "sleep",
    "stream_from_queue_sentinel",
    "timeout",
    "wait",
    "wait_for",
    "yield_control",
)

logger = logging.getLogger(__name__)
_T = TypeVar("_T")
# 使用 ContextVar 存储当前事件循环（线程安全、协程安全）
_loop_var: ContextVar[Optional["EventLoop"]] = ContextVar("_loop", default=None)
# 用于存储当前正在运行的任务（线程安全、协程安全）
_task_var: ContextVar[Optional["Task"]] = ContextVar("_task", default=None)

FIRST_COMPLETED = "FIRST_COMPLETED"
FIRST_EXCEPTION = "FIRST_EXCEPTION"
ALL_COMPLETED = "ALL_COMPLETED"

_return_when = Literal["FIRST_COMPLETED", "FIRST_EXCEPTION", "ALL_COMPLETED"]


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


def get_running_loop_safe() -> Optional["EventLoop"]:
    """安全获取当前正在运行的事件循环，若无则返回 None"""
    try:
        return get_running_loop()
    except RuntimeError:
        return None


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


class FutureCancelledError(Exception):
    """任务或 Future 被取消时抛出的异常"""

    pass


class AsyncTimeoutError(Exception):
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


class Future(Generic[_T]):
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

    def set_result(self, result: _T) -> None:
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

    def result(self) -> _T:
        """
        获取 Future 的结果

        Returns:
            _T: 操作结果

        Raises:
            RuntimeError: 如果 Future 还未完成
            Exception: 如果 Future 有异常，则抛出该异常
        """
        self._check_done(False)
        if self._exception is not None:
            raise self._exception
        return self._result

    def __iter__(self) -> Generator["Future[_T]", Any, Any]:
        """
        让 Future 可被 yield，生成器通过 yield future 来等待

        Returns:
            Generator: 生成器对象
        """
        result = yield self
        return result

    def __await__(self) -> Generator["Future[_T]", Any, Any]:
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
        self._finish(exc=FutureCancelledError(msg))
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


class Task(Future[_T]):
    """
    异步任务，将生成器或协程包装成 Future

    Task 继承自 Future，负责驱动生成器/协程一步步执行。
    它管理任务的调度、取消和完成状态。

    Attributes:
        _gen: 被包装的生成器或协程对象
        _name: 任务名称（用于调试和日志）
        _next_value: 下一次发送给生成器的值
        _stepping: 是否正在执行 _step（防止重入）
        _rescheduled: 是否已重新调度（处理重入情况）
        _waiting: 当前等待的 Future 对象
    """

    __slots__ = (
        "_gen",
        "_name",
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
        self._gen = gen
        self._name = name or f"Task-{id(self)}"  # 任务名称
        # 启动生成器，并处理它遇到的第一个 yield
        self._loop.call_soon(self._step)
        self._next_value: Any = None
        self._stepping = False
        self._rescheduled = False
        self._waiting: Optional[Future] = None  # 当前等待的 Future
        # 延迟复制 Context：在真正第一次运行任务时再复制，避免创建大量不必要的 Context
        self._context = None

    def get_name(self) -> str:
        """
        获取任务名称

        Returns:
            str: 任务名称
        """
        return self._name

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
        return self._done and isinstance(self._exception, FutureCancelledError)

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

    @staticmethod
    def current_task() -> Optional["Task"]:
        """
        获取当前正在运行的任务

        Returns:
            Task 或 None: 当前任务（如果有）
        """
        return _task_var.get()

    def _step(self, value: Any = None, is_exc: bool = False) -> None:
        """
        在上下文运行 _real_step 方法
        """

        def run_step():
            token = _task_var.set(self)
            try:
                self._real_step(value, is_exc)
            finally:
                _task_var.reset(token)

        # 延迟在首次执行时复制 contextvars 上下文，降低 Task 创建开销
        if self._context is None:
            # 在首次运行时复制当前上下文
            self._context = contextvars.copy_context()
        self._context.run(run_step)

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
                yielded = self._gen.throw(value)
            elif self._cancelled:
                self._cancelled = False
                self._next_value = None  # 清掉 resume 值（关键！）
                yielded = self._gen.throw(FutureCancelledError(self._cancel_msg))

            else:
                send_value = self._next_value
                self._next_value = None
                yielded = self._gen.send(send_value)
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
        self._ready: deque[tuple[Callable[..., Any], Tuple[Any, ...]]] = (
            deque()
        )  # 就绪回调队列
        self._scheduled: list[tuple[float, int, TimerHandle]] = (
            []
        )  # 最小堆，存 (deadline, seq, handler)
        self._timer_seq = 0
        self._running = False
        self._tasks: Set[Task] = set()
        self._selector = selectors.DefaultSelector()
        self._exception_handler: Optional[Callable[["EventLoop", dict], None]] = None
        self._execute_pool: Optional[ThreadPoolExecutor] = None
        self._ready_lock = threading.Lock()  # 保护 _ready 的线程锁
        self._closed = False  # 标记是否已关闭
        self._thread_wakeup_poll_interval = (
            0.01  # self-pipe 被限制时的跨线程唤醒兜底轮询间隔
        )

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

    def is_running(self) -> bool:
        """
        检查事件循环是否正在运行

        Returns:
            bool: 如果正在运行返回 True
        """
        return self._running

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
        current_loop = get_running_loop_safe()
        # 仅在跨线程路径需要唤醒事件循环
        if current_loop is not self:
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
        if self._closed:
            raise RuntimeError("Event loop is closed")
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
        if self._closed:
            raise RuntimeError("Event loop is closed")
        deadline = time.monotonic() + delay
        self._timer_seq += 1
        handler = TimerHandle(callback, args, self, deadline)
        heapq.heappush(self._scheduled, (deadline, self._timer_seq, handler))
        return handler

    def add_reader(
        self,
        fileobj: int | socket.socket,
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
        fileobj: int | socket.socket,
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

    def remove_reader(self, fileobj: int | socket.socket) -> None:
        """
        移除文件描述符的可读事件

        Args:
            fileobj: 文件描述符或 socket
        """
        self._remove_io_callbacks("read", fileobj)

    def remove_writer(self, fileobj: int | socket.socket) -> None:
        """
        移除文件描述符的可写事件

        Args:
            fileobj: 文件描述符或 socket
        """
        self._remove_io_callbacks("write", fileobj)

    def _fd(self, fileobj: int | socket.socket) -> int:
        """返回文件描述符整数。"""
        return fileobj if isinstance(fileobj, int) else fileobj.fileno()

    def _add_io_callback(
        self,
        direction: str,
        fileobj: int | socket.socket,
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
        # 尝试读取当前注册信息以决定是否需要调用 modify/unregister
        if not new_events:  # 想要注销
            try:
                self._selector.unregister(fd)
            except KeyError:
                pass
            return

        # new_events != 0：需要注册或修改
        try:
            key = self._selector.get_key(fd)
        except KeyError:
            # 当前未注册，直接注册
            try:
                self._selector.register(fd, new_events, data)
            except Exception:
                pass
            return
        # 已注册，只在事件集或 data 对象发生变化时才 modify
        if key.events != new_events or key.data is not data:
            try:
                self._selector.modify(fd, new_events, data)
            except KeyError:
                pass

    def _remove_io_callbacks(
        self, direction: str, fileobj: int | socket.socket
    ) -> None:
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
        self, direction: str, fileobj: int | socket.socket, callback: Callable[..., Any]
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
        fileobj: int | socket.socket,
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
        fileobj: int | socket.socket,
        callback: Callable[..., Any],
        *args: Any,
        one_shot: bool = False,
    ) -> None:
        """线程安全地注册可写回调（可从其他线程调用）。"""
        self.call_soon_threadsafe(
            self._add_io_callback, "write", fileobj, callback, args, one_shot
        )

    def remove_reader_callback(
        self, fileobj: int | socket.socket, callback: Callable[..., Any]
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
        self, fileobj: int | socket.socket, callback: Callable[..., Any]
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
        gen: Union[Generator[Future[_T], Any, Any], Awaitable[_T]],
        name: Optional[str] = None,
    ) -> Task[_T]:
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
        task.add_done_callback(self._task_done)  # noqa
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
        self, *coro_s: Union[Awaitable[_T], Generator[Future[_T], Any, Any], Task[_T]]
    ) -> Future[_T]:
        """
        便捷方法：调用模块级 `gather`。
        """
        return gather(*coro_s)

    def wait(
        self,
        fs: List[Union[Awaitable[_T], Generator[Future[_T], Any, Any], Task[_T]]],
        timeout_delay: Optional[float] = None,
        return_when: _return_when = ALL_COMPLETED,
    ) -> Future[_T]:
        """
        便捷方法：调用模块级 `wait`。
        """
        return wait(fs, timeout_delay=timeout_delay, return_when=return_when)

    def wait_for(
        self,
        aw: Union[Awaitable[_T], Generator[Future[_T], Any, Any]],
        timeout_delay: float,
    ) -> Future[_T]:
        """
        便捷方法：调用模块级 `wait_for`。
        """
        return wait_for(aw, timeout_delay=timeout_delay)

    def run(
        self, awaitable: Union[Generator[Future[_T], Any, Any], Task[_T], Future[_T]]
    ) -> _T:
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

    def run_until_complete(self, main_task: Task[_T]) -> _T:
        self._running = True
        main_task.add_done_callback(lambda _: self.stop())
        self._run_loop(main_task)  # 第一阶段：主循环（直到主任务完成）

        # 取消所有其他未完成的任务
        self._cancel_other_tasks(main_task)

        # 第二阶段：清理循环。
        # 此时 main_task 已经 done，我们需要继续运行循环直到 self._tasks 为空
        # 以确保所有任务的 CancelledError 和 finally 块执行完毕。
        if self._tasks:
            self._running = True
            self._run_loop(None)
        return main_task.result()

    def _report_error(self, message: str, exc: Optional[Exception], **kwargs) -> None:
        """
        统一的异常报告入口，构建上下文并调用异常处理器。

        Args:
            message: 错误描述消息
            exc: 捕获到的异常对象
            **kwargs: 额外的上下文信息（如 callback, fd, task 等）
        """
        context = {
            "message": message,
            "exception": exc,
        }
        context.update(kwargs)
        self.call_exception_handler(context)

    def _complete_a_task(self, cb, *args):
        try:
            cb(*args)
        except Exception as e:
            self._report_error("Exception in callback", e, callback=cb, args=args)

    def _dispatch_selector_event(self, key: selectors.SelectorKey, mask: int) -> None:
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

        # 收集待排队的回调，稍后批量入队以减少锁争用
        to_schedule: list[tuple[Callable[..., Any], Tuple[Any, ...]]] = []
        for direction, event_flag in (
            ("read", selectors.EVENT_READ),
            ("write", selectors.EVENT_WRITE),
        ):
            if not (mask & event_flag):
                continue

            # 遍历原始列表以避免每次分配 callback_list，收集需要移除的一次性条目
            lst = data.get(direction, [])
            if not lst:
                continue
            to_remove: list[tuple[Callable[..., Any], Tuple[Any, ...], bool]] = []
            for item in lst:
                cb, args, one_shot = item
                to_schedule.append((cb, args))
                if one_shot:
                    to_remove.append(item)
            if to_remove:
                try:
                    data[direction][:] = [
                        it for it in data.get(direction, []) if it not in to_remove
                    ]
                except Exception:
                    for item in to_remove:
                        try:
                            data[direction].remove(item)
                        except ValueError:
                            pass

        # 批量将回调加入就绪队列（在锁内一次性操作以减少争用）
        if to_schedule:
            with self._ready_lock:
                self._ready.extend(to_schedule)

        # 更新 selector 注册（封装以避免重复代码）
        self._update_selector_registration(fd, data)

    def _flush_ready(self) -> None:
        """原子化地取出当前所有就绪的回调并执行。"""
        if not self._ready:
            return

        with self._ready_lock:
            ready_to_process = self._ready
            self._ready = deque()

        while ready_to_process:
            cb, args = ready_to_process.popleft()
            self._complete_a_task(cb, *args)

    def _run_loop(self, main_task: Optional[Task] = None):
        """
        核心循环驱动引擎。

        该方法通过 while True 持续驱动事件循环，直到满足特定的退出条件。它支持两种运行模式：
        1. 主任务模式 (main_task is not None): 运行直到该任务完成。
        2. 清理模式 (main_task is None): 运行直到所有后台任务 (self._tasks) 彻底清空。

        退出判定优先级：
        - 显式停止：self.stop() 被调用且就绪队列已排空。
        - 模式驱动：主任务完成或所有后台任务处理完毕（包括 finally 块产生的收尾任务）。

        Args:
            main_task: 可选的目标任务。如果提供，循环将以该任务的完成为首要退出信号。
        """
        while True:
            # 核心退出判定：
            should_break = False
            if not self._ready:
                if not self._running:
                    should_break = True
                elif main_task and main_task.done():
                    should_break = True
                elif not self._tasks:
                    should_break = True

            if should_break:
                # --- 最终防御：快照式排空就绪队列 ---
                self._flush_ready()
                break

            # --- 2. 动力源阶段 ---
            # 如果循环已经 stop，则跳过所有外部 I/O 和定时器，只求排空存量就绪任务
            if self._running:
                now = time.monotonic()

                # 2.1 检查定时器
                while self._scheduled and self._scheduled[0][0] <= now:
                    _, _, handler = heapq.heappop(self._scheduled)
                    if handler.cancelled():
                        continue
                    handler.run()

                # 2.2 计算 Select 超时时间
                _timeout: float | None = None
                if self._ready:
                    _timeout = 0.0
                elif self._scheduled:
                    _timeout = max(self._scheduled[0][0] - now, 0.0)
                elif len(self._selector.get_map()) <= 1:
                    _timeout = self._thread_wakeup_poll_interval

                # 2.3 监听 I/O 事件
                events = self._selector.select(_timeout)
                for key, mask in events:
                    try:
                        self._dispatch_selector_event(key, mask)
                    except Exception as e:
                        self._report_error(
                            "Exception while dispatching selector event",
                            e,
                            key=key,
                            mask=mask,
                        )

            # --- 3. 执行阶段 ---
            self._flush_ready()

            # --- 4. 动力源衔接与防御性退出 ---
            # 如果循环已经停止 (stop() 被调用)，处理完这一波 ready 任务后必须退出
            if not self._running:
                break

    def _cancel_other_tasks(self, main_task):
        for t in list(self._tasks):
            if t is not main_task:
                t.cancel()

    def stop(self):
        self._running = False

    def ensure_task(
        self,
        task: Union[
            Awaitable[_T], Generator[Future[_T], Any, Any], Task[_T], Future[_T]
        ],
    ) -> Task[_T]:
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


def sleep(delay: float) -> Future[None]:
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


def run(awaitable: Union[Awaitable[_T], Generator[Future[_T], Any, Any]]) -> _T:
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


def gather(
    *coro_s: Union[Awaitable[_T], Generator[Future[_T], Any, Any], Task[_T]]
) -> Future[List[_T]]:
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
    all_done = loop.create_future()
    if not coro_s:
        all_done.set_result([])
        return all_done

    n = len(coro_s)
    results = [None] * n
    completed = 0
    failed = False
    tasks: dict[Task, int] = {}

    def on_task_done(t: Task):
        nonlocal completed, failed
        if failed:
            return
        idx = tasks.pop(t, None)
        if idx is None:  # 理论上不应发生，但防御
            return
        try:
            results[idx] = t.result()
            completed += 1
            if completed == n:
                all_done.set_result(results)
        except Exception as e:
            failed = True
            for other in tasks:
                if not other.done():
                    other.cancel(msg=f"Gather failed due to task {other._name}")
            all_done.set_exception(e)

    def on_gather_cancel(f: Future):
        if f.cancelled():
            for t in tasks:
                if not t.done():
                    t.cancel(msg="Gather cancelled by parent")

    for i, coro in enumerate(coro_s):
        task = loop.ensure_task(coro)
        tasks[task] = i
        task.add_done_callback(on_task_done)  # noqa

    all_done.add_done_callback(on_gather_cancel)
    return all_done


def wait(
    fs: List[
        Union[Awaitable[_T], Generator[Future[_T], Any, Any], Task[_T], Future[_T]]
    ],
    timeout_delay: Optional[float] = None,
    return_when: _return_when = ALL_COMPLETED,
) -> Future[Tuple[Set[Future[_T]], Set[Future[_T]]]]:
    """
    等待一组任务完成

    类似 asyncio.wait()，等待多个异步操作完成，
    并根据 return_when 参数决定何时返回。

    Args:
        fs: 要等待的协程、生成器或 Task 列表
        timeout_delay: 超时时间（秒），None 表示不超时
        return_when: 返回条件：
            - FIRST_COMPLETED: 第一个任务完成时返回
            - FIRST_EXCEPTION: 第一个任务异常或全部完成时返回
            - ALL_COMPLETED: 所有任务完成时返回（默认）

    Returns:
        Future: 结果是 (done_set, pending_set) 元组
            - done_set: 已完成的任务集合
            - pending_set: 未完成的任务集合

    Example:
        >>> done, pending = await wait([task1, task2], timeout_delay=5.0)
        >>> for task in done:
        ...     print(task.result())
    """
    loop = get_running_loop()
    wait_future = loop.create_future()
    done = set()
    if not fs:
        wait_future.set_result((done, set()))
        return wait_future
    ensure_task = loop.ensure_task
    pending = {f if isinstance(f, Future) else ensure_task(f) for f in fs}
    timeout_handle = None

    def _on_completion(task):
        if wait_future.done():
            return

        pending.discard(task)
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
    for t in list(pending):
        if t.done():
            _on_completion(t)
        else:
            t.add_done_callback(_on_completion)

    # 处理超时逻辑
    if timeout_delay is not None:

        def _on_timeout():
            if not wait_future.done():
                wait_future.set_result((done, pending))

        timeout_handle = loop.call_later(timeout_delay, _on_timeout)

    return wait_future


def wait_for(
    aw: Union[Awaitable[_T], Generator[Future[_T], Any, Any]], timeout_delay: float
) -> Future[_T]:
    """
    等待单个异步操作完成，带超时限制

    类似 asyncio.wait_for()，如果操作在 timeout 秒内未完成，
    则取消该操作并抛出 TimeoutError。

    Args:
        aw: 要等待的协程、生成器或 Task
        timeout_delay: 超时时间（秒）

    Returns:
        Future: 异步操作的结果

    Raises:
        TimeoutError: 如果操作超时

    Example:
        >>> async def slow_task():
        ...     await sleep(10)
        ...     return "done"
        >>> try:
        ...     result = await wait_for(slow_task(), timeout_delay=1.0)
        ... except AsyncTimeoutError:
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
            task.cancel(msg=f"Timeout after {timeout_delay}s")  # 超时自动取消
            done_future.set_exception(AsyncTimeoutError())

    timeout_handle = loop.call_later(timeout_delay, _on_timeout)

    # 如果任务提前完成，取消定时器
    def cancel_timeout(fut: Future):
        if fut.done() and not timeout_handle.cancelled():
            timeout_handle.cancel()

    done_future.add_done_callback(cancel_timeout)

    return done_future


def create_task(
    aw: Union[Awaitable[_T], Generator[Future[_T], Any, Any]], name: str = None
) -> Task[_T]:
    """
    创建一个 Task 对象

    Args:
        aw: 要包装的协程、生成器或 Future
        name: 任务的名称，用于调试和日志记录
    Returns:
        Task: 包装后的 Task 对象
    """
    loop = get_running_loop()
    return loop.create_task(aw, name=name)


class _TimeoutContext:
    """
    异步超时上下文管理器

    用于为一段异步代码块提供超时保护。如果代码块执行时间超过指定值，
    将抛出 TimeoutError。

    Example:
        >>> async with timeout(1.5):
        ...     await slow_operation()
    """

    def __init__(self, delay: float) -> None:
        """
        初始化超时上下文

        Args:
            delay: 超时延迟时间（秒）
        """
        self._delay = delay
        self._loop = get_running_loop()
        self._timer: TimerHandle | None = None
        self._task: Task | None = None
        self._timed_out = False

    async def __aenter__(self):
        self._task = Task.current_task()
        if not self._task:
            raise RuntimeError("timeout() 必须在异步 Task 内部使用")

        def on_timeout():
            self._timed_out = True
            self._task.cancel(msg=f"Timeout after {self._delay}s")

        self._timer = self._loop.call_later(self._delay, on_timeout)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._timer:
            self._timer.cancel()
        if exc_type is FutureCancelledError and self._timed_out:
            raise AsyncTimeoutError() from exc_val


def timeout(delay: float) -> _TimeoutContext:
    """
    创建一个异步超时上下文管理器

    Args:
        delay: 超时秒数
    Returns:
        _TimeoutContext: 上下文管理器实例
    """
    return _TimeoutContext(delay)


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


class QueueFullError(Exception):
    """
    队列已满异常
    """

    pass


class QueueEmptyError(Exception):
    """
    队列为空异常
    """

    pass


class Event:
    """
    异步事件实现

    允许多个协程等待某个事件的发生。当事件被 set() 时，所有等待的协程都会被唤醒。
    实现采用多 Future 模式，支持任务取消且互不干扰。
    """

    def __init__(self) -> None:
        """初始化事件，默认为未设置状态"""
        self._loop = get_running_loop()
        self._waiters: deque[Future[None]] = deque()
        self._is_set = False

    def is_set(self) -> bool:
        """如果事件已设置则返回 True"""
        return self._is_set

    def set(self) -> None:
        """
        设置事件，唤醒所有等待者
        之后调用 wait() 的协程将不会阻塞。
        """
        if self.is_set():
            return
        self._is_set = True
        while self._waiters:
            f = self._waiters.popleft()
            if not f.done():
                f.set_result(None)

    async def wait(self) -> bool:
        """
        等待事件被设置
        如果事件已设置，则立即返回 True。
        """
        if self.is_set():
            return True
        f = self._loop.create_future()
        self._waiters.append(f)
        try:
            await f
            return True
        finally:
            try:
                self._waiters.remove(f)
            except ValueError:
                pass

    def clear(self) -> None:
        """重置事件为未设置状态，之后调用 wait() 的协程将会阻塞"""
        self._is_set = False


class BaseAsyncLock:
    """
    异步线程安全锁基类
    提供跨线程安全的操作事件循环的能力，是所有自定义异步锁的底层支撑。
    """

    __slots__ = ("_event", "_loop")

    def __init__(self):
        # 记录初始化时的事件循环，用于跨线程安全调度
        self._loop = get_running_loop_safe()
        self._event = Event()
        # 默认初始状态为放行状态
        self._event.set()

    def _safe_dispatch(self, action_name: str):
        """通用跨线程安全调度器"""
        current_loop = get_running_loop_safe()

        # 获取对应的 Event 方法 (set 或 clear)
        action = getattr(self._event, action_name)
        loop = self._loop
        if loop and loop.is_running() and loop is not current_loop:
            loop.call_soon_threadsafe(action)
        else:
            action()

    def _wake_up(self):
        """跨线程安全地唤醒所有等待者"""
        self._safe_dispatch("set")

    def _pause(self):
        """跨线程安全地挂起所有等待者（进入锁定状态）"""
        self._safe_dispatch("clear")

    async def wait_unlock(self):
        """异步挂起等待，直到计时归零或被强制解锁"""
        await self._event.wait()

    def is_locked(self) -> bool:
        """检查当前是否处于锁定状态"""
        return not self._event.is_set()


class AsyncToggleLock(BaseAsyncLock):
    """
    异步开关锁：提供跨线程安全的“开启/关闭”控制信号。
    适用于 Worker 暂停/恢复、系统开关等场景。
    """

    def activate(self):
        """打开开关（放行状态）"""
        self._wake_up()

    def deactivate(self):
        """关闭开关（阻塞状态）"""
        self._pause()

    async def wait_for_active(self):
        """异步等待直到开关被打开"""
        await self.wait_unlock()


class AsyncCountdownLock(BaseAsyncLock):
    """
    通用异步计数解锁器 (Asyncio WaitGroup 模式)

    在高并发异步环境下实现“多发一收”的计数屏障。类似于 Go 语言的 sync.WaitGroup，
    支持同步加减计数，并提供异步等待点。

    设计特性：
    1. 极致性能：使用 __slots__ 优化内存，核心计数操作均为同步方法，避免协程调度开销。
    2. 健壮性：内置超额释放(Double-Release)防御和自动状态纠正。
    3. 追踪性：支持自动追踪 Task 生命周期，彻底防止死锁。
    4. 灵活：支持上下文管理器(Context Manager)模式。
    """

    __slots__ = ("_unlock_count",)

    def __init__(self, count: int = 0):
        """
        初始化计数器。
        Args:
            count: 初始计数。如果 > 0，则初始状态为锁定；如果为 0，则为放行。
        """
        super().__init__()
        self._unlock_count = count
        if count > 0:
            self._pause()

    def acquire(self):
        """
        同步增加计数(锁定)。
        一旦计数大于 0，wait_unlock 会进入阻塞状态。
        """
        self._unlock_count += 1
        if self._event.is_set():
            self._pause()

    def release(self):
        """
        同步减少计数(解锁)。
        如果计数降至 0 或以下，将触发事件唤醒所有等待者。
        """
        self._unlock_count -= 1
        if self._unlock_count <= 0:
            self._unlock_count = 0
            # 确保跨线程安全
            self._wake_up()

    def force_unlock(self):
        """
        【紧急动作】强制清零计数并解锁。
        """
        self._unlock_count = 0
        self._wake_up()

    def is_locked(self) -> bool:
        """检查当前是否有任务正在运行(红灯状态)"""
        return not self._event.is_set()

    @property
    def lock_count(self) -> int:
        """获取当前正在锁定的任务数量"""
        return self._unlock_count

    def __enter__(self):
        """支持 context manager 同步锁定进入"""
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """支持 context manager 同步锁定退出，即便发生异常也能确保释放"""
        self.release()

    def trace_task(self, task: Task) -> bool:
        """
        自动追踪一个异步 Task。
        通过 add_done_callback 机制，在 Task 结束(成功/失败/取消)时自动 release。

        Args:
            task: 需要被追踪的 Task 对象

        Returns:
            bool: 如果任务尚未完成并成功挂载回调返回 True，否则返回 False
        """
        if task.done():
            return False
        self.acquire()
        task.add_done_callback(lambda _: self.release())
        return True


class AsyncQueue(Generic[_T]):
    """
    异步队列实现

    支持并发的生产者和消费者，提供可选的容量限制。
    当队列满时，put 操作会阻塞；当队列空时，get 操作会阻塞。

    Attributes:
        _items: 存储队列元素的 deque
        _getters: 等待 get 操作的 Future 队列
        _putters: 等待 put 操作的 (item, Future) 队列
        _maxsize: 队列最大容量，0 表示无限
    """

    def __init__(self, maxsize: int = 0) -> None:
        """
        初始化异步队列

        Args:
            maxsize: 队列最大容量，0 表示不设限制
        """
        self._loop = get_running_loop()
        self._items: deque[_T] = deque()
        self._getters: deque[Future[_T]] = deque()
        self._putters: deque[Tuple[_T, Future[None]]] = deque()
        self._maxsize = maxsize
        self._unfinished_tasks = AsyncCountdownLock()

    def qsize(self) -> int:
        """返回队列中的元素数量"""
        return len(self._items)

    def empty(self) -> bool:
        """如果队列为空则返回 True"""
        return not self._items

    def full(self) -> bool:
        """如果队列已满则返回 True"""
        return 0 < self._maxsize <= len(self._items)

    def _wake_getter(self, item: _T) -> bool:
        """
        唤醒等待 get 操作的 Future
        循环查找非完成的 getter，设置其结果为 item 并返回 True。
        如果所有 getter 都已完成（如被取消），返回 False。
        """
        while self._getters:
            getter = self._getters.popleft()
            if not getter.done():
                getter.set_result(item)
                return True
        return False

    async def put(self, item: _T) -> None:
        """
        向队列中添加一个元素

        如果队列已满，则等待直到有可用空间。

        Args:
            item: 要添加的元素
        """
        if self._wake_getter(item):
            self._unfinished_tasks.acquire()
            return
        if not self.full():
            self._items.append(item)
            self._unfinished_tasks.acquire()
            return
        fut = self._loop.create_future()
        self._putters.append((item, fut))
        await fut
        self._unfinished_tasks.acquire()

    def put_nowait(self, item: _T) -> None:
        """
        非阻塞地向队列添加元素

        Args:
            item: 要添加的元素

        Raises:
            QueueFullError: 如果队列已满
        """
        if self._wake_getter(item):
            self._unfinished_tasks.acquire()
            return

        if self.full():
            raise QueueFullError("Queue full")

        self._items.append(item)
        self._unfinished_tasks.acquire()

    async def get(self) -> _T:
        """
        从队列中获取一个元素

        如果队列为空，则等待直到有元素可用。

        Returns:
            _T: 获取到的元素
        """
        if self._items:
            item = self._items.popleft()
            self._wake_putter()
            return item
        fut = self._loop.create_future()
        self._getters.append(fut)
        return await fut

    def get_nowait(self) -> _T:
        """
        非阻塞地从队列获取元素

        Returns:
            _T: 获取到的元素

        Raises:
            QueueEmptyError: 如果队列为空
        """
        if not self._items:
            raise QueueEmptyError("Queue empty")

        item = self._items.popleft()
        self._wake_putter()
        return item

    def task_done(self) -> None:
        """
        通知队列之前排队的任务已完成
        由消费者在处理完任务后调用。
        """
        self._unfinished_tasks.release()

    async def join(self) -> None:
        """
        阻塞直到队列中所有任务都被处理完成
        即所有 put() 进来的任务都调用了对应的 task_done()。
        """
        await self._unfinished_tasks.wait_unlock()

    def _wake_putter(self):
        """
        唤醒等待 put 操作的 Future
        循环查找非完成的 putter，将其 item 放入队列或直接交给等待的 getter。
        """
        while self._putters:
            item, fut = self._putters.popleft()
            if fut.done():
                continue

            if not self._wake_getter(item):
                self._items.append(item)

            fut.set_result(None)
            break


class WaiterEntry:
    __slots__ = ("target_ids", "fut", "count")

    def __init__(
        self,
        target_ids: frozenset,
        fut: Future[None] | None,
        count: Optional[int] = None,
    ):
        """
        初始化等待者条目。

        Args:
            target_ids: 目标 ID 集合
            count: 需要解锁的 ID 数量（默认全部）
            fut: 完成时触发的 Future，或 None 表示无触发
        """
        self.target_ids: frozenset[int] = target_ids
        self.fut: Future[None] | None = fut
        # 若未指定 count，则默认需要全部 ID 解锁 (即解锁数量 >= 总数)
        self.count: int = count if count is not None else len(target_ids)

    def __repr__(self) -> str:
        return f"_WaiterEntry(target_ids={self.target_ids}, fut={self.fut}, count={self.count})"


class SelectiveLockBase(BaseAsyncLock):
    """
    选择性锁基类：提供基于自增 ID 的局部等待机制。

    封装 _waiters 队列管理、ID 自增分配及核心等待逻辑，
    子类须实现 _is_any_active(target_ids) 以定义"活跃"语义。
    """

    __slots__ = ("_waiters", "_waiters_all", "_id_counter")

    def __init__(self):
        super().__init__()
        self._id_counter: int = 0
        # _waiters: dict mapping tid -> set of _WaiterEntry instances
        self._waiters: dict[int, set[WaiterEntry]] = {}
        # _waiters_all: set containing all _WaiterEntry instances (for full-scan operations)
        self._waiters_all: set[WaiterEntry] = set()

    def _next_id(self) -> int:
        """分配下一个唯一 ID"""
        self._id_counter += 1
        return self._id_counter

    def _is_any_active(self, target_ids: set[int] | frozenset[int]) -> bool:
        """检查目标 ID 中是否还有活跃的（子类实现）"""
        raise NotImplementedError

    def _remove_waiter(self, entry: WaiterEntry):
        """将等待者从全局集合和 tid 索引中移除（内部工具）"""
        self._waiters_all.discard(entry)
        for tid in entry.target_ids:
            s = self._waiters.get(tid)
            if s:
                s.discard(entry)
                if not s:
                    del self._waiters[tid]

    def _is_condition_met(self, entry: WaiterEntry) -> bool:
        """检查等待者是否满足唤醒条件（子类实现）"""
        return not self._is_any_active(entry.target_ids)

    def _try_wake_waiter(self, entry: WaiterEntry) -> bool:
        """检查等待者是否满足唤醒条件，若满足则唤醒并清理索引"""
        if self._is_condition_met(entry):
            if not entry.fut.done():
                entry.fut.set_result(None)
            self._remove_waiter(entry)
            return True
        return False

    def _trigger_waiters_check(self):
        """全量检查所有等待者（用于回退或状态恢复）"""
        if not self._waiters_all:
            return

        # 使用 list 拷贝以支持在遍历中移除
        for entry in list(self._waiters_all):
            self._try_wake_waiter(entry)

    def _trigger_waiters_for_t_ids(self, t_ids: set[int]):
        """精准触发：仅检查受给定 t_ids 影响的等待者"""
        if not self._waiters_all:
            return

        impacted: set[WaiterEntry] = set()
        for tid in t_ids:
            s = self._waiters.get(tid)
            if s:
                impacted.update(s)

        for entry in list(impacted):
            self._try_wake_waiter(entry)

    async def _wait_for_ids(
        self, target_set: set[int], count: Optional[int] = None
    ) -> None:
        """核心挂起逻辑：将协程加入等待队列并阻塞"""
        if not target_set:
            await self._event.wait()
            return

        # 初始检查
        entry_temp = WaiterEntry(frozenset(target_set), None, count)
        if self._is_condition_met(entry_temp):
            return

        fut = get_running_loop().create_future()
        entry = WaiterEntry(frozenset(target_set), fut, count)

        # 注册索引
        self._waiters_all.add(entry)
        for tid in entry.target_ids:
            self._waiters.setdefault(tid, set()).add(entry)

        try:
            await fut
        finally:
            # 无论是因为成功被唤醒还是因为超时/取消，都要确保清理索引
            self._remove_waiter(entry)

    async def _on_force(self, target_set: set[int]) -> None:
        """超时强制清理钩子（子类实现）"""
        raise NotImplementedError

    async def wait_unlock(
        self,
        target_ids: Union[list[int], set[int], None] = None,
        duration: Optional[float] = None,
        force: bool = False,
    ):
        """
        等待解锁。
        Args:
        - target_ids=None: 全局屏障，等待所有 ID 完成。
        - target_ids=[...]: 局部屏障，只要指定 ID 完成即放行。
        - duration: 超时等待时间（秒）。
        - force: 若超时，是否调用 _on_force 执行强制清理。
        """
        target_set = set(target_ids) if target_ids is not None else set()
        if duration is not None:
            try:
                async with timeout(duration):
                    await self._wait_for_ids(target_set)
            except AsyncTimeoutError:
                if force:
                    try:
                        await self._on_force(target_set)
                    except Exception as e:
                        logging.error(
                            f"{self.__class__.__name__}: Error during force action: {e}"
                        )
                raise
        else:
            await self._wait_for_ids(target_set)

    def acquire(self, task_identifier: Any) -> int:
        """
        一个任务标识符，可自动分配或指定一个。标识锁定了一段事务，可以是任务或代码块。
        Returns:
            int: 分配给该 task_identifier 的唯一 tid，用于后续 release/wait_unlock
        """
        tid = self._next_id()
        if self._event.is_set():
            self._pause()
        return tid

    def release(self, tid: int):
        """释放指定 tid 的锁"""
        raise NotImplemented

    def trace_task(self, task: Task) -> int:
        """
        追踪 Task 生命周期：自动加锁，任务结束时自动 release。
        Returns:
            int: 分配给该 Task 的唯一 tid
        """

        tid = self.acquire(task)
        task.add_done_callback(lambda _: self.release(tid))
        return tid


class AsyncSelectiveLock(SelectiveLockBase):
    """
    高级异步策略锁-支持细粒度局部解锁

    基于 ID 追踪机制，允许协程等待一组并行任务中的“特定子集”完成，而无需等待全部任务结束。
    兼顾了全量等待(Barrier)和局部等待(Partial Wait)的能力。
    """

    # 协程私有口袋：存储当前上下文中领到的 ID，确保 with 语法下的 ID 传递安全
    _current_tid: ContextVar[Optional[int]] = ContextVar("_current_tid", default=None)

    def __enter__(self) -> int:
        """支持 with lock: 语法，自动领票并存入口袋"""
        tid = self.acquire()
        self._current_tid.set(tid)
        return tid

    def __exit__(self, exc_type, exc_val, exc_tb):
        """支持 with lock: 语法，自动从口袋摸票并销账"""
        tid = self._current_tid.get()
        if tid is not None:
            self.release(tid)
            self._current_tid.set(None)

    async def __aenter__(self) -> int:
        """支持 async with lock: 语法"""
        return self.__enter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """支持 async with lock: 语法"""
        return self.__exit__(exc_type, exc_val, exc_tb)

    @contextmanager
    def hold(self, task_id: Optional[int] = None):
        """显式上下文管理器：with lock.hold(id) as tid:"""
        tid = self.acquire(task_id)
        try:
            yield tid
        finally:
            self.release(tid)

    __slots__ = ("_active_ids",)

    def __init__(self):
        super().__init__()
        self._active_ids: set[int] = set()

    def _is_any_active(self, target_ids: set[int]) -> bool:
        return bool(target_ids & self._active_ids)

    def acquire(self, task_id: Optional[int] = None) -> int:
        """增加一个带标识的锁定任务，若未提供 task_id 则生成并返回一个自增 ID"""
        if task_id is None:
            task_id = self._next_id()

        if task_id in self._active_ids:
            return task_id

        self._active_ids.add(task_id)
        if self._event.is_set():
            self._pause()
        return task_id

    def release(self, task_id: int):
        """释放指定标识的任务，并驱动唤醒相关的局部等待者"""
        if task_id not in self._active_ids:
            return

        self._active_ids.discard(task_id)

        # 1. 局部解锁逻辑：仅触发与该 tid 有关的等待者，避免全表扫描
        try:
            self._trigger_waiters_for_t_ids({task_id})
        except AttributeError:
            # 回退：若索引尚未建立，则做全表检查以保证兼容性
            self._trigger_waiters_check()

        # 2. 全局解锁逻辑
        if not self._active_ids:
            self._wake_up()

    def _is_condition_met(self, entry: WaiterEntry) -> bool:
        """精准检查解锁数量是否达到阈值"""
        locked_ids = self.locked_ids(entry.target_ids)
        available_count = len(entry.target_ids) - len(locked_ids)
        return available_count >= entry.count

    async def wait_count(
        self,
        target_ids: Union[list[int], set[int]],
        count: int,
        duration: Optional[float] = None,
        force: bool = False,
    ) -> set[int]:
        """
        等待 target_ids 中的至少 count 个 ID 处于解锁状态。

        Args:
            target_ids: 关注的 ID 集合
            count: 期望解锁的最小数量
            duration: 最大等待时间
            force: 超时是否强制解锁

        Returns:
            set[int]: 当前已解锁的 ID 集合
        """
        target_set = set(target_ids)
        if count > len(target_set):
            raise ValueError("count cannot be greater than the number of target_ids")

        if duration is not None:
            try:
                async with timeout(duration):
                    await self._wait_for_ids(target_set, count=count)
            except AsyncTimeoutError:
                if force:
                    await self._on_force(target_set)
                raise
        else:
            await self._wait_for_ids(target_set, count=count)

        # 返回当前已解锁的 ID
        return target_set - self.locked_ids(target_set)

    def trace_task(self, task: Task) -> int:
        """追踪 Task 对象生命周期并返回其唯一追踪 ID"""
        tid = self.acquire()
        task.add_done_callback(lambda _: self.release(tid))
        return tid

    async def _on_force(self, target_set: set[int]) -> None:
        """超时时强制解锁：从活跃集中移除目标 ID。"""
        self.force_unlock(target_set if target_set else None)

    def force_unlock(self, target_ids: Union[list[int], set[int], None] = None):
        """
        强制解锁：
        - target_ids=None: 灾难恢复，全量清空所有状态并放行。
        - target_ids=[...]: 局部强拆。将指定的 ID 强行移出活跃集，并同步驱动等待者检查。
        """
        if target_ids is None:
            # 全量强拆：清空活跃集合并放行所有等待者
            self._active_ids.clear()
            for entry in list(self._waiters_all):
                if not entry.fut.done():
                    entry.fut.set_result(None)
            self._waiters_all.clear()
            self._waiters.clear()
            self._wake_up()
        else:
            target_set = set(target_ids)
            changed = False
            for tid in target_set:
                if tid in self._active_ids:
                    self._active_ids.discard(tid)
                    changed = True

            if changed:
                # 仅触发受影响的等待者
                try:
                    self._trigger_waiters_for_t_ids(target_set)
                except AttributeError:
                    self._trigger_waiters_check()

            if not self._active_ids:
                self._wake_up()

    @property
    def active_ids(self) -> set[int]:
        """获取当前所有活跃（锁定）的 ID 集合拷贝"""
        return set(self._active_ids)

    def locked_ids(
        self, target_ids: Union[list[int], set[int], frozenset[int]]
    ) -> set[int]:
        """
        查询指定的多个 ID 是否还在锁定状态。

        Args:
            target_ids: 要查询的 ID 集合。

        Returns:
            set[int]: 传入 ID 中依然处于锁定状态的子集。
        """
        return set(target_ids) & self._active_ids

    def is_locked(self, task_id: Optional[int] = None) -> bool:
        """检查状态：若指定 id 则检查特定任务，否则检查全局锁"""
        if task_id is not None:
            return task_id in self._active_ids
        return bool(self._active_ids)


class AsyncSemaphore:
    """
    异步信号量：控制同时访问资源的协程数量。

    通过内部计数器管理资源余量：
    - acquire(): 减少计数，若计数为 0 则等待。
    - release(): 增加计数，并唤醒等待者。
    """

    __slots__ = ("_value", "_waiters")

    def __init__(self, value: int = 1):
        """
        Args:
            value: 初始余量（并发数），默认为 1。
        """
        if value < 0:
            raise ValueError("Semaphore initial value must be >= 0")
        self._value = value
        self._waiters: deque[Future[None]] = deque()

    def locked(self) -> bool:
        """若没有余量，则返回 True"""
        return self._value == 0

    async def acquire(self) -> bool:
        """
        获取信号量。
        如果计数器大于 0，则减 1 并立即返回。
        如果计数器为 0，则挂起直到被 release 唤醒。
        """
        if self._value <= 0:
            fut = get_running_loop().create_future()
            self._waiters.append(fut)
            try:
                await fut
                return True
            except FutureCancelledError:
                if not fut.done() and fut in self._waiters:
                    self._waiters.remove(fut)
                raise

        self._value -= 1
        return True

    def release(self):
        """
        释放信号量。
        增加计数器，并尝试唤醒队列头部的第一个有效等待者。
        """
        self._value += 1
        while self._waiters:
            fut = self._waiters.popleft()
            if not fut.done():
                # 令牌直接转交给被唤醒的协程
                self._value -= 1
                fut.set_result(None)
                break

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.release()


class AsyncLock(AsyncSemaphore):
    """
    异步互斥锁（Mutex）：AsyncSemaphore(1) 的特化版本。

    保证同一时间只有一个协程能持有该锁。
    """

    def __init__(self):
        super().__init__(value=1)

    def __repr__(self):
        status = "locked" if self.locked() else "unlocked"
        return f"<AsyncLock [{status}] waiters={len(self._waiters)}>"


async def stream_from_queue_sentinel(
    queue: AsyncQueue[_T],
    end_signal: Union[Task, Event],
    filter_func: Optional[Callable[[_T], bool]] = None,
    name: str = "StreamPump",
) -> AsyncGenerator[_T, None]:
    """
    通用异步流式泵：从异步队列中流式获取数据，直到接收到结束信号或哨兵(哨兵模式)。

    该工具函数提供了一种高性能、内存友好的方式来消费异步队列。它结合了“哨兵模式”和“贪婪消费模式”，
    旨在最大化吞吐量并最小化协程调度开销。

    工作原理：
    1. 信号监听：通过为 Task 绑定回调或为 Event 绑定等待任务回调，在结束时向队列投放哨兵对象。
    2. 贪婪消费：进入循环后，优先通过 get_nowait() 排空队列中的所有存量数据，减少 yield 导致的调度切换。
    3. 阻塞等待：当队列为空时，使用 await queue.get() 挂起，直到下一个结果或哨兵到来。
    4. 异常传导：运行失败，函数会将异常上抛。

    Args:
        queue: 要消费的异步队列 (AsyncQueue)。
        end_signal: 结束信号源，可以是 Task (任务结束) 或 Event (事件设置)。
        filter_func: 可选的结果过滤函数。如果提供，只有通过过滤的结果才会被 yield。
        name: 用于描述该流式泵的名称（用于日志和调试）。

    Yields:
        从队列中获取的原始数据。

    Raises:
        Exception: 当 end_signal 是 Task 且其执行过程中出现异常时。
        FutureCancelledError: 当外部显式取消该生成器迭代时。
    """

    # 局部内部函数，用于安全投放哨兵到队列
    def safe_put_sentinel(_=None):
        try:
            queue.put_nowait(stop_sentinel)
            logging.debug(f"{name}: Sent sentinel to queue")
        except QueueFullError:
            # 如果队列由于某种原因满了，稍后重试，确保哨兵一定能送达
            try:
                loop = get_running_loop()
                logging.warning(f"{name}: Queue full, retrying to put sentinel...")
                loop.call_soon(safe_put_sentinel)  # noqa
            except RuntimeError:
                pass  # 循环可能已关闭

    if not isinstance(end_signal, (Task, Event)):
        logging.error(f"end_signal type error: {type(end_signal)}")
        raise TypeError("end_signal must be of Task or Event type")
    stop_sentinel = object()
    not_filter = filter_func is None
    if isinstance(end_signal, Event):
        task = create_task(end_signal.wait(), name=f"{name}_EventMonitor")
    else:
        task = end_signal

    # 挂载完成回调
    task_name = task.get_name()
    if task.done():
        logging.debug(f"{name}: Task {task_name} is done, putting sentinel to queue.")
        safe_put_sentinel()
    else:
        logging.debug(f"{name}: Task {task_name} is not done, waiting for completion.")
        task.add_done_callback(safe_put_sentinel)

    try:
        while True:
            # 1. 尝试获取 item：先尝试非阻塞 grab，失败则转为阻塞 await
            try:
                item = queue.get_nowait()
            except QueueEmptyError:
                item = await queue.get()

            # 2. 统一处理 item 逻辑
            if item is stop_sentinel:
                while True:
                    try:
                        n_item = queue.get_nowait()
                        if not_filter or filter_func(n_item):
                            yield n_item
                        queue.task_done()
                    except QueueEmptyError:
                        break
                queue.task_done()
                # 收到哨兵，检查 Task 异常（如果有）并退出
                exc = task.exception()
                if exc:
                    raise exc
                return

            if not_filter or filter_func(item):
                yield item
            queue.task_done()
            # logging.debug(f"{name}: Task {item.task_name} consumed.")

    except FutureCancelledError:
        # 外部取消了迭代
        logging.error(
            f"{name}: The general-purpose asynchronous flow pump was manually cancelled."
        )
        try:
            task.cancel()
            await task
        except FutureCancelledError:
            logging.warning(
                f"{name}: The consumer task {task_name} was cancelled synchronously."
            )
        raise
    finally:
        logging.info(f"{name}: The general-purpose asynchronous flow pump completed.")
