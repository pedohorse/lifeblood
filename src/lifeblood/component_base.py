import asyncio


class ComponentBase:
    def __init__(self):
        super().__init__()
        self.__stop_event = asyncio.Event()
        self.__main_task = None

    @property
    def _stop_event(self):
        return self.__stop_event

    def start(self):
        self.__main_task = asyncio.create_task(self._main_task())

    def stop(self):
        self.__stop_event.set()

    async def wait_till_stops(self):
        if self.__main_task is None:
            return
        return await self.__main_task

    def _main_task(self):
        """
        should return the coroutine that will produce the main task to run by the component
        """
        raise NotImplementedError('override this with the main task')
