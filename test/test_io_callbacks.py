import socket
import threading
import time

from simple_asyncio import EventLoop


def test_remove_reader_callback():
    print("\n=== test_remove_reader_callback ===")
    loop = EventLoop()
    s1, s2 = socket.socketpair()
    s1.setblocking(False)
    s2.setblocking(False)

    def cb(fut):
        try:
            fut.set_result(True)
        except Exception:
            pass

    # 1) 回调被触发
    f = loop.create_future()
    loop.add_reader(s1, cb, f)
    s2.send(b"x")
    res = loop.run(f)
    assert res is True
    print("callback invoked -> OK")

    # 2) 回调被移除后不再触发（使用短定时器作为超时替代）
    f2 = loop.create_future()
    loop.add_reader(s1, cb, f2)
    loop.remove_reader_callback(s1, cb)
    loop.call_later(0.05, lambda: f2.set_result(False))
    s2.send(b"y")
    res2 = loop.run(f2)
    assert res2 is False
    print("callback removed -> OK")

    s1.close()
    s2.close()


def test_add_reader_threadsafe():
    print("\n=== test_add_reader_threadsafe ===")
    loop = EventLoop()
    s1, s2 = socket.socketpair()
    s1.setblocking(False)
    s2.setblocking(False)

    def cb(fut):
        try:
            fut.set_result("threaded")
        except Exception:
            pass

    f = loop.create_future()
    # 防止 loop 在 worker 注册回调前认为没有任务而提前退出，添加一个短定时器保持循环活动
    loop.call_later(0.5, lambda: None)

    def worker():
        # 在独立线程中注册回调并触发写入
        time.sleep(0.01)
        loop.add_reader_threadsafe(s1, cb, f)
        time.sleep(0.01)
        s2.send(b"z")

    t = threading.Thread(target=worker)
    t.start()

    res = loop.run(f)
    t.join()
    assert res == "threaded"
    print("add_reader_threadsafe -> OK")

    s1.close()
    s2.close()


def test_remove_writer_callback():
    print("\n=== test_remove_writer_callback ===")
    loop = EventLoop()
    s1, s2 = socket.socketpair()
    s1.setblocking(False)
    s2.setblocking(False)

    def cb(fut):
        try:
            fut.set_result("writer")
        except Exception:
            pass

    # 注册可写回调（socketpair 很快可写），然后移除，确保不被触发
    f = loop.create_future()
    loop.add_writer(s1, cb, f)
    loop.remove_writer_callback(s1, cb)
    # 使用定时器作为超时判断
    loop.call_later(0.05, lambda: f.set_result(False))
    res = loop.run(f)
    assert res is False
    print("remove_writer_callback -> OK")

    s1.close()
    s2.close()


if __name__ == "__main__":
    test_remove_reader_callback()
    test_add_reader_threadsafe()
    test_remove_writer_callback()
    print("\nAll IO callback tests passed!")
