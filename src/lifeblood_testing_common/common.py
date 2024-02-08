
async def chain(*coros):
    for coro in coros:
        await coro
