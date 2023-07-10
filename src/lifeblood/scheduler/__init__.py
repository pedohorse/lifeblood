import sys
import os
from pathlib import Path
import asyncio
import signal
from ..defaults import scheduler_port as default_scheduler_port, ui_port as default_ui_port
from ..config import get_config, create_default_user_config_file, get_local_scratch_path
from .. import logging
from .. import paths

from .scheduler import Scheduler

from typing import Optional


default_config = f'''
[core]
## you can uncomment stuff below to specify some static values
## 
# server_ip = "192.168.0.2"
# server_port = {default_scheduler_port()}
# ui_ip = "192.168.0.2"
# ui_port = {default_ui_port()}

## you can turn off scheduler broadcasting if you want to manually configure viewer and workers to connect
## to a specific address
# broadcast = false

[scheduler]

[scheduler.globals]
## entries from this section will be available to any node from config[key] 
##
## if you use more than 1 machine - you must change this to a network location shared among all workers
## by default it's set to scheduler's machine local temp path, and will only work for 1 machine setup 
global_scratch_location = "{get_local_scratch_path()}"

[scheduler.database]
## you can specify default database path, 
##  but this can be overriden with command line argument --db-path
# path = "/path/to/database.db"

## uncomment line below to store task logs outside of the database
##  it works in a way that all NEW logs will be saved according to settings below
##  existing logs will be kept where they are
##  external logs will ALWAYS be looked for in location specified by store_logs_externally_location
##  so if you have ANY logs saved externally - you must keep store_logs_externally_location defined in the config, 
##    or those logs will be inaccessible
##  but you can safely move logs and change location in config accordingly, but be sure scheduler is not accessing them at that time
# store_logs_externally = true
# store_logs_externally_location = /path/to/dir/where/to/store/logs
'''


async def main_async(db_path=None, *, broadcast_interval: Optional[int] = None):
    def graceful_closer(*args):
        scheduler.stop()

    noasync_do_close = False
    def noasync_windows_graceful_closer_event(*args):
        nonlocal noasync_do_close
        noasync_do_close = True

    async def windows_graceful_closer():
        while not noasync_do_close:
            await asyncio.sleep(1)
        graceful_closer()

    scheduler = Scheduler(db_path,
                          do_broadcasting=broadcast_interval > 0 if broadcast_interval is not None else None,
                          broadcast_interval=broadcast_interval)
    win_signal_waiting_task = None
    try:
        asyncio.get_event_loop().add_signal_handler(signal.SIGINT, graceful_closer)
        asyncio.get_event_loop().add_signal_handler(signal.SIGTERM, graceful_closer)
    except NotImplementedError:  # solution for windows
        signal.signal(signal.SIGINT, noasync_windows_graceful_closer_event)
        signal.signal(signal.SIGBREAK, noasync_windows_graceful_closer_event)
        win_signal_waiting_task = asyncio.create_task(windows_graceful_closer())

    await scheduler.start()
    await scheduler.wait_till_stops()
    if win_signal_waiting_task is not None:
        if not win_signal_waiting_task.done():
            win_signal_waiting_task.cancel()
    logging.get_logger('scheduler').info('SCHEDULER STOPPED')


def main(argv):
    import argparse
    import tempfile

    parser = argparse.ArgumentParser('lifeblood scheduler')
    parser.add_argument('--db-path', help='path to sqlite database to use')
    parser.add_argument('--ephemeral', action='store_true', help='start with an empty one time use database, that is placed into shared memory IF POSSIBLE')
    parser.add_argument('--verbosity-pinger', help='set individual verbosity for worker pinger')
    parser.add_argument('--broadcast-interval', type=int, help='help easily override broadcasting interval (in seconds). value 0 disables broadcasting')
    opts = parser.parse_args(argv)

    # check and create default config if none
    create_default_user_config_file('scheduler', default_config)

    config = get_config('scheduler')
    if opts.db_path is not None:
        db_path = opts.db_path
    else:
        db_path = config.get_option_noasync('scheduler.database.path', str(paths.default_main_database_location()))

    global_logger = logging.get_logger('scheduler')

    fd = None
    if opts.ephemeral:
        if opts.db_path is not None:
            parser.error('only one of --db-path or --ephemeral must be provided, not both')
        # 'file:memorydb?mode=memory&cache=shared'
        # this does not work ^ cuz shared cache means that all runs on the *same connection*
        # and when there is a transaction conflict on the same connection - we get instalocked (SQLITE_LOCKED)
        # and there is no way to emulate normal DB in memory but with shared cache

        # look for shm (UNIX only)
        shm_path = Path('/dev/shm')
        lb_shm_path = None
        if shm_path.exists():
            lb_shm_path = shm_path/f'u{os.getuid()}-lifeblood'
            try:
                lb_shm_path.mkdir(exist_ok=True)
            except Exception as e:
                global_logger.warning('/dev/shm is not accessible (permission issues?), creating ephemeral database in temp dir')
                lb_shm_path = None
        else:
            global_logger.warning('/dev/shm is not supported by OS, creating ephemeral database in temp dir')

        fd, db_path = tempfile.mkstemp(dir=lb_shm_path, prefix='shedb-')

    if opts.verbosity_pinger:
        logging.get_logger('scheduler.worker_pinger').setLevel(opts.verbosity_pinger)
    try:
        asyncio.run(main_async(db_path, broadcast_interval=opts.broadcast_interval))
    except KeyboardInterrupt:
        global_logger.warning('SIGINT caught')
        global_logger.info('SIGINT caught. Scheduler is stopped now.')
    finally:
        if opts.ephemeral:
            assert fd is not None
            os.close(fd)
            os.unlink(db_path)


if __name__ == '__main__':
    main(sys.argv[1:])
