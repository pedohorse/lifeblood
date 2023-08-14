import asyncio


class ComponentBase:
    def __init__(self):
        super().__init__()
        self.__stop_event = asyncio.Event()
        self.__main_task = None
        self.__main_task_is_ready = asyncio.Event()

    @property
    def _stop_event(self):
        return self.__stop_event

    def _main_task_is_ready_now(self):
        """
        subclass needs to call this once starting initializations are done
        to signal that component is in running state
        """
        self.__main_task_is_ready.set()

    async def start(self):
        if self.__main_task is not None:
            raise RuntimeError('already started')
        self.__main_task = asyncio.create_task(self._main_task())
        ready_waiter = asyncio.create_task(self.__main_task_is_ready.wait())
        done, others = await asyncio.wait([ready_waiter, self.__main_task], return_when=asyncio.FIRST_COMPLETED)
        if self.__main_task in done:  # means it raised an error
            for other in others:
                other.cancel()
            await self.__main_task

    def stop(self):
        if self.__main_task is None:
            raise RuntimeError('not started')
        self.__stop_event.set()

    async def wait_till_stops(self):
        await self.__stop_event.wait()
        return await self.__main_task

    def _main_task(self):
        """
        should return the coroutine that will produce the main task to run by the component
        """
        raise NotImplementedError('override this with the main task')
