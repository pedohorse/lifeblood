import asyncio
from .component_base import ComponentBase
from .logging import get_logger
from multiprocessing import Process, get_context
from multiprocessing.connection import Connection
from threading import Event

from typing import Optional, Tuple


def rx_recv(rx, ev: Event):
    while not rx.poll(0.1):
        if ev.is_set():
            return None
    return rx.recv()


async def target_async(component: ComponentBase, rx: Connection, log_level: int):
    logger = get_logger(f'detached_component.{type(component).__name__}')
    logger.setLevel(log_level)
    logger.debug('component starting...')
    await component.start()
    logger.debug('component started')

    exit_ev = Event()
    stop_task = asyncio.get_event_loop().run_in_executor(None, rx_recv, rx, exit_ev)
    done_task = asyncio.create_task(component.wait_till_stops())
    done, _ = await asyncio.wait(
        [
            done_task,
            stop_task
        ],
        return_when=asyncio.FIRST_COMPLETED,
    )
    if done_task in done:
        exit_ev.set()
        if stop_task in done:
            await stop_task  # reraise exceptions if any happened
        else:
            stop_task.cancel()
    elif stop_task in done:
        logger.debug('component received stop message')
        component.stop()
        logger.debug('component stop called')
        await done_task
    else:
        raise RuntimeError('unreachable')
    rx.close()
    logger.debug('component finished')


def target(component: ComponentBase, rx: Connection, log_level: int):
    asyncio.run(target_async(component, rx, log_level))


class ComponentProcessWrapper:
    _context = get_context('spawn')

    def __init__(self, component_to_run: ComponentBase):
        """
        component_to_run must not be started
        """
        self.__component = component_to_run
        self.__proc: Optional[Process] = None
        self.__comm_sender: Optional[Connection] = None

    async def start(self):
        rx, tx = self._context.Pipe(False)  # type: Tuple[Connection, Connection]

        self.__comm_sender = tx
        log_level = get_logger('detached_component').level
        self.__proc = self._context.Process(target=target, args=(self.__component, rx, log_level))
        self.__proc.start()

    def stop(self):
        if self.__proc is None:
            raise RuntimeError('not started')
        if not self.__comm_sender.closed:
            try:
                self.__comm_sender.send(0)
            except OSError:  # rx might close beforehand
                pass
            self.__comm_sender.close()

    async def wait_till_stops(self):
        if self.__proc is None:
            raise RuntimeError('not started')
        # better poll for now,
        #  alternative would be using a dedicated 1-thread pool executor and wait there
        while self.__proc.exitcode is None:
            # what is this random polling time?
            await asyncio.sleep(2.5)
        if not self.__comm_sender.closed:
            self.__comm_sender.close()
