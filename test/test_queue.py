from simple_asyncio import run, sleep, gather, AsyncQueue


async def producer(q: AsyncQueue):
    for i in range(5):
        await sleep(0.1)
        await q.put(i)
        print("put", i)


async def consumer(q: AsyncQueue):
    for _ in range(5):
        item = await q.get()
        print("get", item)


async def main():
    q: AsyncQueue = AsyncQueue()
    await gather(producer(q), consumer(q))


if __name__ == "__main__":
    run(main())
