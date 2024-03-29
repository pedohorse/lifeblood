import asyncio
from ..component_base import ComponentBase
from ..enums import SchedulerMode

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # TODO: maybe separate a subset of scheduler's methods to smth like SchedulerData class, or idunno, for now no obvious way to separate, so having a reference back
    from .scheduler import Scheduler


class SchedulerComponentBase(ComponentBase):
    def __init__(self, scheduler: "Scheduler"):
        super().__init__()
        self.__stop_event = asyncio.Event()
        self.__main_task = None
        self.__wakeup_event = asyncio.Event()
        self.__scheduler = scheduler
        self.__mode = SchedulerMode.STANDARD

    @property
    def scheduler(self) -> "Scheduler":
        return self.__scheduler

    @property
    def mode(self):
        return self.__mode

    @property
    def _poke_event(self):
        """
        poke event for the components to wait on, like
        self._poke_event().wait()

        when something calls `poke()` - all waiter will be awoken
        """
        return self.__wakeup_event

    def poke(self):
        """
        poke scheduler components to interrupt sleep and continue pinging immediately.
        """
        self.__wakeup_event.set()
        # set() finishes all current waiters, so it's safe to clear() it straight after.
        #  BEWARE: i'm not sure if this is asyncio's intended logic or implementation details
        self.__wakeup_event.clear()

    def _main_task(self):
        raise NotImplementedError('override this with the main task')

    def _my_sleep(self):
        raise NotImplementedError('override this')

    def _my_wake(self):
        raise NotImplementedError('override this')

    def sleep(self):
        if self.__mode == SchedulerMode.DORMANT:
            return
        self.__mode = SchedulerMode.DORMANT
        self._my_sleep()
        self.scheduler._component_changed_mode(self, self.__mode)

    def wake(self):
        if self.__mode == SchedulerMode.STANDARD:
            return
        self.__mode = SchedulerMode.STANDARD
        self._my_wake()
        self.scheduler._component_changed_mode(self, self.__mode)
