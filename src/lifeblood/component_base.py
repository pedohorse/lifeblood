import asyncio


class _aio_Event_placeholder:
    def __init__(self, event_set: bool):
        self.event_set = event_set


class _aio_Lock_placeholder:
    def __init__(self, locked: bool):
        self.locked = locked


class ComponentBase:
    def __init__(self):
        super().__init__()
        self.__start_event = asyncio.Event()
        self.__stop_event = asyncio.Event()
        self.__main_task = None
        self.__main_task_is_ready = asyncio.Event()

    def __getstate__(self):
        """
        this cheat is required for pythons <3.10
        where some aio objects are NOT pickleable.
        And we want to be pickleable for multiprocessing

        This is rough, and does not cover all cases

        This is TO BE DEPRECATED when python 3.8 3.9 are deprecated
        """
        state = self.__dict__.copy()
        stash = {}
        for k, v in state.items():
            if not isinstance(v, (asyncio.Event, asyncio.Lock)):
                continue
            obj_id = id(v)
            if obj_id in stash:
                state[k] = stash[obj_id]
            else:
                if isinstance(v, asyncio.Event):
                    placeholder = _aio_Event_placeholder(v.is_set())
                elif isinstance(v, asyncio.Lock):
                    placeholder = _aio_Lock_placeholder(v.locked())
                else:
                    raise RuntimeError('unreachable')
                state[k] = placeholder
                stash[obj_id] = placeholder

        return state

    def __setstate__(self, state):
        """
        read __getstate__
        """
        state = state.copy()
        stash = {}
        for k, v in state.items():
            if not isinstance(v, (_aio_Event_placeholder, _aio_Lock_placeholder)):
                continue
            obj_id = id(v)
            if obj_id in stash:
                state[k] = stash[obj_id]
            else:
                if isinstance(v, _aio_Event_placeholder):
                    placeholder = asyncio.Event()
                    if v.event_set:
                        placeholder.set()
                elif isinstance(v, _aio_Lock_placeholder):
                    placeholder = asyncio.Lock()
                    if v.locked:
                        placeholder._locked = True
                else:
                    raise RuntimeError('unreachable')
                state[k] = placeholder
                stash[obj_id] = placeholder

        self.__dict__.update(state)

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
            await self.__main_task  # exception re-raised here
        self.__start_event.set()

    def stop(self):
        if self.__main_task is None:
            raise RuntimeError('not started')
        self.__stop_event.set()

    async def wait_till_stops(self):
        await self.__start_event.wait()
        return await self.__main_task

    def _main_task(self):
        """
        should return the coroutine that will produce the main task to run by the component
        """
        raise NotImplementedError('override this with the main task')
