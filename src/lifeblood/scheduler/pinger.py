import asyncio
from datetime import datetime
from .. import logging
from ..message_processor_ping_generic_handler import PingGenericClient
from .scheduler_component_base import SchedulerComponentBase
from .ping_producer_base import PingEntity, PingProducerBase, PingEntityIdleness, PingReply
from ..net_messages.address import AddressChain
from ..net_messages.exceptions import MessageTransferError, MessageTransferTimeoutError

from typing import Dict, Iterable, List, Tuple, TYPE_CHECKING

if TYPE_CHECKING:  # TODO: maybe separate a subset of scheduler's methods to smth like SchedulerData class, or idunno, for now no obvious way to separate, so having a reference back
    from .scheduler_core import SchedulerCore


class Pinger(SchedulerComponentBase):
    def __init__(
            self,
            scheduler: "SchedulerCore",
            ping_processors: Iterable[PingProducerBase],
    ):
        super().__init__(scheduler)
        self.__pinger_logger = logging.get_logger('scheduler.worker_pinger')

        self.__ping_interval, self.__ping_idle_interval, self.__ping_off_interval, self.__dormant_mode_ping_interval_multiplier = self.scheduler.config_provider.ping_intervals()
        self.__ping_interval_mult = 1

        self.__processors: List[PingProducerBase] = list(ping_processors)

    def _main_task(self):
        return self.pinger()

    def _my_sleep(self):
        self.__ping_interval_mult = self.__dormant_mode_ping_interval_multiplier

    def _my_wake(self):
        self.__ping_interval_mult = 1
        self.poke()

    def set_pinger_interval_multiplier(self, multiplier: float):
        self.__ping_interval_mult = multiplier

    async def _ping_awaiter(self, entity: PingEntity) -> PingReply:
        exc = None
        data = None
        try:
            with PingGenericClient.get_worker_control_client(entity.address(), self.scheduler.message_processor()) as client:  # type: PingGenericClient
                data = (await client.ping(entity.ping_data()))['data']
        except MessageTransferTimeoutError as e:
            self.__pinger_logger.info(f'    :: network timeout {entity.address()}')
            exc = e
        except MessageTransferError as e:
            self.__pinger_logger.info(f'    :: host/route down {entity.address()} {e.wrapped_exception()}')
            exc = e
        except Exception as e:
            self.__pinger_logger.info(f'    :: ping failed {entity.address()} {type(e)}, {e}')
            exc = e

        return PingReply(
            entity,
            data,
            exc,
        )

    #
    # pinger task
    async def pinger(self):
        """
        one of main constantly running coroutines
        responsible for pinging all the workers once in a while in separate tasks each
        TODO: test how well this approach works for 1000+ workers
        """

        tasks: Dict[AddressChain, Tuple[asyncio.Task, PingEntity, PingProducerBase]] = {}
        stop_task = asyncio.create_task(self._stop_event.wait())
        wakeup_task = asyncio.create_task(self._poke_event.wait())
        poll_task = None
        self._main_task_is_ready_now()
        while not self._stop_event.is_set():
            nowtime = datetime.now()  # TODO: use utc time!

            self.__pinger_logger.debug('    ::selecting pingables...')
            entities = [(x, processor) for processor in self.__processors for x in await processor.select_entities()]
            self.__pinger_logger.debug('    ::selected pingables: %d from %d producers', len(entities), len(self.__processors))
            stat_discarded = 0
            stat_attempted = 0
            for entity, processor in entities:
                if entity.address() in tasks:  # if we are already waiting for a reply from this address - do not pile them up
                    await processor.entity_discarded(entity)
                    stat_discarded += 1
                    continue

                time_delta = (nowtime - entity.last_checked()).total_seconds()
                if (entity.idleness() == PingEntityIdleness.ACTIVE
                        or entity.idleness() == PingEntityIdleness.WORKING_IDLE and time_delta > self.__ping_idle_interval * self.__ping_interval_mult
                        or entity.idleness() == PingEntityIdleness.SLEEPING_IDLE and time_delta > self.__ping_off_interval * self.__ping_interval_mult):
                    await processor.entity_accepted(entity)
                    tasks[entity.address()] = (asyncio.create_task(self._ping_awaiter(entity)), entity, processor)
                    stat_attempted += 1
                else:
                    await processor.entity_discarded(entity)
                    stat_discarded += 1

            self.__pinger_logger.debug('    ::from selected pingables: %d attempted, %d discarded', stat_attempted, stat_discarded)

            while True:
                # now clean the list
                pruned_tasks = {}
                for key, (task, entity, processor) in tasks.items():
                    if task.done():
                        reply = await task  # _ping_awaiter is not supposed to raise
                        await processor.entity_reply_received(reply)
                    else:
                        pruned_tasks[key] = (task, entity, processor)
                tasks = pruned_tasks
                self.__pinger_logger.debug('    :: remaining ping tasks: %d', len(tasks))

                # now wait
                if poll_task is None:
                    poll_task = asyncio.create_task(asyncio.sleep(self.__ping_interval * self.__ping_interval_mult))
                if wakeup_task is None:
                    wakeup_task = asyncio.create_task(self._poke_event.wait())
                sleeping_tasks = (stop_task, wakeup_task, poll_task)

                done, _ = await asyncio.wait(
                    sleeping_tasks + tuple(x[0] for x in tasks.values()),  # wait on stopping tasks OR any ping to finish
                    timeout=2 * self.__ping_interval * self.__ping_interval_mult,  # this timeout is really arbitrary, we don't need it really
                    return_when=asyncio.FIRST_COMPLETED
                )
                if len(done) == 0:  # timeout happened
                    continue

                if wakeup_task in done:
                    wakeup_task = None
                if poll_task in done:
                    poll_task = None
                if wakeup_task is None or poll_task is None:
                    break  # and continue outer while loop if stop not set

                # end when stop is set
                if stop_task in done:
                    break
                # if not breaked - one of ping tasks have completed, so we continue inner loop
            if stop_task in done:
                break

        # FINALIZING PINGER
        self.__pinger_logger.info('finishing worker pinger...')
        if poll_task and not poll_task.done():
            poll_task.cancel()
        if not wakeup_task.done():
            wakeup_task.cancel()
        if not stop_task.done():
            stop_task.cancel()
        if len(tasks) > 0:
            self.__pinger_logger.debug(f'waiting for {len(tasks)} pinger tasks...')
            t_done, t_pending = await asyncio.wait([x[0] for x in tasks.values()], return_when=asyncio.ALL_COMPLETED, timeout=5)
            self.__pinger_logger.debug(f'waiting enough, {len(t_done)} tasks finished properly, cancelling {len(t_pending)} tasks')
            for _, (task, entity, processor) in tasks.items():
                # discard all!
                await processor.entity_discarded(entity)
                if task in t_done:
                    await task
                    t_done.remove(task)
                elif task in t_pending:
                    task.cancel()
                    t_pending.remove(task)
            assert len(t_pending) == 0, t_pending
            assert len(t_done) == 0, t_done
        self.__pinger_logger.info('worker pinger finished')
