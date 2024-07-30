import asyncio
import json
import os.path
import signal
from . import logging
from .nethelpers import get_default_addr
from .broadcasting import await_broadcast
from .config import get_config, create_default_user_config_file, Config
from .enums import WorkerType, ProcessPriorityAdjustment
from .net_messages.address import AddressChain
from .worker import Worker

from typing import Optional

default_config = '''
[worker]
listen_to_broadcast = true

[default_env_wrapper]
## here you can uncomment lines below to specify your own default environment wrapper and default arguments
## this will only be used by invocation jobs that have NO environment wrappers specified
# name = TrivialEnvironmentResolver
# arguments = [ "project_name", "or", "config_name", "idunno", "maybe rez packages requirements?", [1,4,11] ]

# [resources]
## here you can override resources that this machine has
## if you don't specify anything - resources will be detected automatically
# cpu_count = 32    # by default treated as the number of cores 
# cpu_mem = "128G"  # you can either specify int amount of bytes, or use string ending with one of "K" "M" "G" "T" "P" meaning Kilo, Mega, Giga, ... 

## If you want lifeblood to control GPU as device resource - you need to uncomment section below
##  adjust values according to your actual hardware
##  don't forget to uncomment device definition section in scheduler's config too.
##  tags are arbitrary key-value pairs available at runtime, some nodes, such as houdini nodes
##  expect certain tags to understand how this device maps to whatever they consider a usable device
##  refer to specific node manual to read about tags they use and their meaning 
# [devices.gpu.gpu1.resources]  # you can name it however you want instead of gpu1
# # be sure to override these values below with actual ones!
# mem = "4G"
# opencl_ver = 3.0
# cuda_cc = 5.0
# [devices.gpu.gpu1.tags]
# houdini_ocl = "GPU::0"
# karma_dev = "0/1"
'''


async def main_async(config: Config,
                     worker_type=WorkerType.STANDARD,
                     child_priority_adjustment: ProcessPriorityAdjustment = ProcessPriorityAdjustment.NO_CHANGE,
                     singleshot: bool = False, worker_id: Optional[int] = None, pool_address=None, noloop=False):
    """
    listen to scheduler broadcast in a loop.
    if received - create the worker and work
    if worker cannot ping the scheduler a number of times - it stops
    and listenting for broadcast starts again
    :return: Never!
    """
    graceful_closer_no_reentry = False

    def graceful_closer(*args):
        nonlocal graceful_closer_no_reentry
        if graceful_closer_no_reentry:
            print('DOUBLE SIGNAL CAUGHT: ALREADY EXITING')
            return
        graceful_closer_no_reentry = True
        logging.get_logger('worker').info('SIGINT/SIGTERM caught')
        nonlocal noloop
        noloop = True
        stop_event.set()
        if worker is not None:
            worker.stop()

    noasync_do_close = False

    def noasync_windows_graceful_closer_event(*args):
        nonlocal noasync_do_close
        noasync_do_close = True

    async def windows_graceful_closer():
        while not noasync_do_close:
            await asyncio.sleep(1)
        graceful_closer()

    worker = None
    stop_event = asyncio.Event()
    win_signal_waiting_task = None
    try:
        asyncio.get_event_loop().add_signal_handler(signal.SIGINT, graceful_closer)
        asyncio.get_event_loop().add_signal_handler(signal.SIGTERM, graceful_closer)
    except NotImplementedError:  # solution for windows
        signal.signal(signal.SIGINT, noasync_windows_graceful_closer_event)
        signal.signal(signal.SIGBREAK, noasync_windows_graceful_closer_event)
        win_signal_waiting_task = asyncio.create_task(windows_graceful_closer())

    logger = logging.get_logger('worker')
    if await config.get_option('worker.listen_to_broadcast', True):
        stop_task = asyncio.create_task(stop_event.wait())
        while True:
            logger.info('listening for scheduler broadcasts...')
            broadcast_task = asyncio.create_task(await_broadcast('lifeblood_scheduler'))
            done, _ = await asyncio.wait((broadcast_task, stop_task), return_when=asyncio.FIRST_COMPLETED)
            if stop_task in done:
                broadcast_task.cancel()
                logger.info('broadcast listening cancelled')
                break
            assert broadcast_task.done()
            message = await broadcast_task
            scheduler_info = json.loads(message)
            logger.debug('received', scheduler_info)
            if 'message_address' not in scheduler_info:
                logger.debug('broadcast does not have "message_address" key, ignoring')
                continue
            addr = AddressChain(scheduler_info['message_address'])
            try:
                worker = Worker(
                    addr,
                    child_priority_adjustment=child_priority_adjustment,
                    worker_type=worker_type,
                    config=config,
                    singleshot=singleshot,
                    worker_id=worker_id,
                    pool_address=pool_address
                )
                await worker.start()  # note that server is already started at this point
            except Exception:
                logger.exception('could not start the worker')
            else:
                await worker.wait_till_stops()
                logger.info('worker quited')
            if noloop:
                break
    else:
        logger.info('boradcast listening disabled')
        while True:
            addr = AddressChain(await config.get_option('worker.scheduler_address', get_default_addr()))
            logger.debug(f'using {addr}')
            try:
                worker = Worker(
                    addr,
                    child_priority_adjustment=child_priority_adjustment,
                    worker_type=worker_type,
                    config=config,
                    singleshot=singleshot,
                    worker_id=worker_id,
                    pool_address=pool_address
                )
                await worker.start()  # note that server is already started at this point
            except ConnectionRefusedError as e:
                logger.exception('Connection error', str(e))
                await asyncio.sleep(10)
                continue
            await worker.wait_till_stops()
            logger.info('worker quited')
            if noloop:
                break

    if win_signal_waiting_task is not None:  # this happens only on windows
        if not win_signal_waiting_task.done():
            win_signal_waiting_task.cancel()
    else:
        asyncio.get_event_loop().remove_signal_handler(signal.SIGINT)  # this seem to fix the bad signal fd error
        asyncio.get_event_loop().remove_signal_handler(signal.SIGTERM)  # my guess what happens is that loop closes, but signal handlers remain if not unsed


def main(argv):
    # import signal
    # prev = None
    # def signal_handler(sig, frame):
    #     print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! You pressed Ctrl+C !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    #     prev(sig, frame)
    #
    # prev = signal.signal(signal.SIGINT, signal_handler)
    import argparse
    parser = argparse.ArgumentParser('lifeblood worker', description='executes invocations from scheduler')
    parser.add_argument('--scheduler-address', help='manually specify scheduler to connect to. if not specified - by default worker will start listening to broadcasts from schedulers')
    parser.add_argument('--no-listen-broadcast', action='store_true', help='do not listen to scheduler\'s broadcast, use config')
    parser.add_argument('--no-loop', action='store_true', help='by default worker will return into the loop of waiting for scheduler every time it quits because of connection loss, or other errors. '
                                                               'but this flag will force worker to just completely quit instead')
    parser.add_argument('--singleshot', action='store_true', help='worker will pick one job and exit after that job is completed or cancelled. '
                                                                  'this is on by default when type=SCHEDULER_HELPER')
    parser.add_argument('--type', choices=('STANDARD', 'SCHEDULER_HELPER'), default='STANDARD')
    parser.add_argument('--id', help='integer identifier which worker should use when talking to worker pool')
    parser.add_argument('--pool-address', help='if this worker is a part of a pool - pool address. currently pool can only be on the same host')
    parser.add_argument('--priority', choices=tuple(x.name for x in ProcessPriorityAdjustment), default=ProcessPriorityAdjustment.NO_CHANGE.name, help='adjust child process priority')
    parser.add_argument('--generate-config-only', action='store_true', help='just generate initial config and exit. Note that existing config will NOT be overriden')
    parser.add_argument('--override-config-path', default=None, help='provide alternative worker config path')

    args = parser.parse_args(argv)

    override_config_dir: Optional[str] = args.override_config_path
    if override_config_dir and not os.path.isabs(override_config_dir):
        override_config_dir = os.path.realpath(override_config_dir)
    # check and create default config if none
    create_default_user_config_file(override_config_dir or 'worker', default_config)

    if args.generate_config_only:
        return

    if args.type == 'STANDARD':
        wtype = WorkerType.STANDARD
    elif args.type == 'SCHEDULER_HELPER':
        wtype = WorkerType.SCHEDULER_HELPER
    else:
        raise NotImplementedError(f'worker type {args.type} is not yet implemented')

    priority_adjustment = [x for x in ProcessPriorityAdjustment if x.name == args.priority][0]  # there MUST be exactly 1 match

    global_logger = logging.get_logger('worker')

    # check and create default config if none
    create_default_user_config_file('worker', default_config)

    # check legality of the address
    paddr = AddressChain(args.pool_address)

    config = get_config(override_config_dir or 'worker')
    if args.no_listen_broadcast:
        config.set_override('worker.listen_to_broadcast', False)
    if args.scheduler_address is not None:
        config.set_override('worker.listen_to_broadcast', False)
        saddr = AddressChain(args.scheduler_address)
        config.set_override('worker.scheduler_address', str(saddr))
    try:
        asyncio.run(main_async(config, wtype, child_priority_adjustment=priority_adjustment, singleshot=args.singleshot, worker_id=int(args.id) if args.id is not None else None, pool_address=paddr, noloop=args.no_loop))
    except KeyboardInterrupt:
        # if u see errors in pycharm around this area when running from scheduler -
        # it's because pycharm and most shells send SIGINTs to this child process on top of SIGINT that pool sends
        # this stuff above tries to suppress that double SIGINTing, but it's not 100% solution
        global_logger.warning('SIGINT caught where it wasn\'t supposed to be caught')
        global_logger.info('SIGINT caught. Worker is stopped now.')


if __name__ == '__main__':
    import sys
    main(sys.argv)
