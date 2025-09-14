import asyncio
from datetime import datetime, timedelta
from ..timestamp import global_timestamp_datetime
from .. import logging
from ..message_processor_ping_generic_handler import PingGenericClient
from .scheduler_component_base import SchedulerComponentBase
from .ping_producer_base import PingEntity, PingProducerBase, PingEntityIdleness, PingReply
from ..net_messages.address import AddressChain
from ..net_messages.exceptions import MessageTransferError, MessageTransferTimeoutError

from typing import Dict, Iterable, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:  # TODO: maybe separate a subset of scheduler's methods to smth like SchedulerData class, or idunno, for now no obvious way to separate, so having a reference back
    from .scheduler_core import SchedulerCore


class Pinger(SchedulerComponentBase):
    def __init__(
            self,
            scheduler: "SchedulerCore",
            ping_producers: Iterable[PingProducerBase],
    ):
        super().__init__(scheduler)
        self.__pinger_logger = logging.get_logger('scheduler.worker_pinger')

        self.__ping_interval, self.__ping_idle_interval, self.__ping_off_interval, self.__dormant_mode_ping_interval_multiplier = self.scheduler.config_provider.ping_intervals()
        self.__ping_interval_mult = 1

        self.__producers: List[PingProducerBase] = list(ping_producers)

    def _main_task(self):
        return self.pinger()

    def _my_sleep(self):
        self.__ping_interval_mult = self.__dormant_mode_ping_interval_multiplier

    def _my_wake(self):
        self.__ping_interval_mult = 1
        self.poke()

    async def _ping_awaiter(self, entity: PingEntity, promised_period: Optional[float]) -> PingReply:
        exc = None
        data = None
        try:
            with PingGenericClient.get_worker_control_client(entity.address(), self.scheduler.message_processor()) as client:  # type: PingGenericClient
                data = (await client.ping(
                    entity.ping_data(),
                    max_seconds_till_next_ping=promised_period,
                ))['data']
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
        promised_ping_intervals: Dict[AddressChain, Tuple[datetime, float]] = {}
        stop_task = asyncio.create_task(self._stop_event.wait())
        wakeup_task = asyncio.create_task(self._poke_event.wait())
        poll_task = None
        promise_wait_task = None
        self._main_task_is_ready_now()

        while not self._stop_event.is_set():
            nowtime = global_timestamp_datetime()

            self.__pinger_logger.debug('    :: selecting pingables...')
            entities = [(x, producer) for producer in self.__producers for x in await producer.select_entities()]
            self.__pinger_logger.debug('    :: selected pingables: %d from %d producers', len(entities), len(self.__producers))
            stat_discarded = 0
            stat_attempted = 0

            # clear promises for entities that are no more
            entity_addresses = set(x[0].address() for x in entities)
            for address in list(promised_ping_intervals.keys()):
                if address not in entity_addresses:
                    self.__pinger_logger.debug('removing promise for an entity that is no more: %s', address)
                    promised_ping_intervals.pop(address)

            for entity, producer in entities:
                if entity.address() in tasks:  # if we are already waiting for a reply from this address - do not pile them up
                    await producer.entity_discarded(entity)
                    stat_discarded += 1

                    # in edge case we might be due on promised second ping already,
                    #  in such case we can only consider promise is already broken, so we remove it
                    address = entity.address()
                    if (address in promised_ping_intervals
                            and nowtime > promised_ping_intervals[address][0] + timedelta(seconds=promised_ping_intervals[address][1])):
                        self.__pinger_logger.warning(f'removing broken ping promise while ping is being delivered {address}')
                        promised_ping_intervals.pop(address)

                    continue

                # calc appropriate ping interval according to entity idleness and current state of pinger
                if entity.idleness() == PingEntityIdleness.ACTIVE:
                    default_ping_interval = self.__ping_interval * self.__ping_interval_mult
                elif entity.idleness() == PingEntityIdleness.WORKING_IDLE:
                    default_ping_interval = self.__ping_idle_interval * self.__ping_interval_mult
                elif entity.idleness() == PingEntityIdleness.SLEEPING_IDLE:
                    default_ping_interval = self.__ping_off_interval * self.__ping_interval_mult
                else:
                    raise NotImplementedError(f'unknown entity idleness state {entity.idleness()}')

                # effective ping interval for address is the min of promised interval and currently calculated one
                #  it's fine if we ping earlier, it's not fine if we ping later than promised
                time_delta = (nowtime - entity.last_checked()).total_seconds()
                promise_needs_fulfilling = False
                if promised_ping_interval_data := promised_ping_intervals.get(entity.address()):
                    promise_needs_fulfilling = nowtime > promised_ping_interval_data[0] + timedelta(seconds=promised_ping_interval_data[1])
                if time_delta >= default_ping_interval or promise_needs_fulfilling:
                    await producer.entity_accepted(entity)
                    ping_interval = default_ping_interval
                    promised_ping_intervals[entity.address()] = nowtime, ping_interval
                    tasks[entity.address()] = (asyncio.create_task(self._ping_awaiter(
                        entity,
                        ping_interval,
                    )), entity, producer)
                    stat_attempted += 1
                else:
                    await producer.entity_discarded(entity)
                    stat_discarded += 1

            self.__pinger_logger.debug('    :: from selected pingables: %d attempted, %d discarded', stat_attempted, stat_discarded)

            # waiting loop
            done = ()
            while True:
                # now clean the list
                pruned_tasks = {}
                for key, (task, entity, producer) in tasks.items():
                    if task.done():
                        reply = await task  # _ping_awaiter is not supposed to raise
                        await producer.entity_reply_received(reply)
                    else:
                        pruned_tasks[key] = (task, entity, producer)
                tasks = pruned_tasks
                self.__pinger_logger.debug('    :: remaining ping tasks: %d', len(tasks))

                # promise waiter only matters within one loop iteration
                if promise_wait_task is not None:
                    promise_wait_task.cancel()
                    promise_wait_task = None

                # now wait
                nowtime = global_timestamp_datetime()
                if len(promised_ping_intervals) > 0:
                    min_promise_interval_remaining = min(interval - (nowtime - start_datatime).total_seconds() for _, (start_datatime, interval) in promised_ping_intervals.items())
                    if min_promise_interval_remaining <= 0:
                        # instantly break this waiting loop: we are late on promises
                        break
                    promise_wait_task = asyncio.create_task(asyncio.sleep(min_promise_interval_remaining))

                if poll_task is None:
                    poll_task = asyncio.create_task(asyncio.sleep(self.__ping_interval * self.__ping_interval_mult))
                if wakeup_task is None:
                    wakeup_task = asyncio.create_task(self._poke_event.wait())

                done, _ = await asyncio.wait(
                    (stop_task, wakeup_task, poll_task)  # wait on stopping tasks
                    + tuple(x[0] for x in tasks.values())  # OR on any ping to finish
                    + ((promise_wait_task,) if promise_wait_task is not None else ()),  # OR on promise
                    return_when=asyncio.FIRST_COMPLETED
                )
                if len(done) == 0:  # timeout happened
                    continue

                if wakeup_task in done:
                    wakeup_task = None
                if poll_task in done:
                    poll_task = None

                if promise_wait_task in done:
                    promise_wait_task = None
                    # yes, not breaking waiting loop, but allow just one more min_promise_interval_remaining check,
                    # and break if it's actually <= 0
                    # this is to allow possibility for imprecise sleeps
                    continue

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
        if promise_wait_task and not promise_wait_task.done():
            promise_wait_task.cancel()
            promise_wait_task = None
        if len(tasks) > 0:
            self.__pinger_logger.debug(f'waiting for {len(tasks)} pinger tasks...')
            t_done, t_pending = await asyncio.wait([x[0] for x in tasks.values()], return_when=asyncio.ALL_COMPLETED, timeout=5)
            self.__pinger_logger.debug(f'waiting enough, {len(t_done)} tasks finished properly, cancelling {len(t_pending)} tasks')
            for _, (task, entity, producer) in tasks.items():
                # discard all!
                await producer.entity_discarded(entity)
                if task in t_done:
                    await task
                    t_done.remove(task)
                elif task in t_pending:
                    task.cancel()
                    t_pending.remove(task)
            assert len(t_pending) == 0, t_pending
            assert len(t_done) == 0, t_done
        self.__pinger_logger.info('worker pinger finished')
