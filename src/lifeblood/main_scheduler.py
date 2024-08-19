import sys
import os
from pathlib import Path
import asyncio
import signal
from .config import get_config, create_default_user_config_file
from .pluginloader import PluginNodeDataProvider
from .scheduler import Scheduler
from .basenode_serializer_v1 import NodeSerializerV1
from .basenode_serializer_v2 import NodeSerializerV2
from .scheduler_config_provider_base import SchedulerConfigProviderBase
from .scheduler_config_provider_file import SchedulerConfigProviderFileOverrides
from . import logging

from typing import Iterable, List, Optional, Tuple, Union


def __construct_plugin_paths(custom_plugins_path: Union[None, str, Path], plugin_search_locations: Iterable[Union[str, Path]]) -> List[Tuple[Path, str]]:
    logger = logging.get_logger('scheduler')
    plugin_paths: List[Tuple[Path, str]] = []  # list of tuples of path to dir, plugin category
    core_plugins_path = Path(__file__).parent / 'core_nodes'
    stock_plugins_path = Path(__file__).parent / 'stock_nodes'

    # "custom" path always comes first, as earlier entries take precedence in case of conflicts
    if custom_plugins_path is not None:
        if isinstance(custom_plugins_path, str):
            custom_plugins_path = Path(custom_plugins_path)
        if not custom_plugins_path.is_absolute():
            custom_plugins_path = custom_plugins_path.absolute()
        # create dir for the "custom package". all changes (preset/settings creation) BY DEFAULT go into this package
        (custom_plugins_path / 'custom_default').mkdir(parents=True, exist_ok=True)
        plugin_paths.append((custom_plugins_path, 'user'))

    # user defined packages come next
    extra_paths = []
    for path in plugin_search_locations:
        if isinstance(path, str):
            path = Path(path)
        if not path.is_absolute():
            logger.warning(f'"{path}" is not absolute, skipping')
            continue
        if not path.exists():
            logger.warning(f'"{path}" does not exist, skipping')
            continue
        extra_paths.append(path)
        logger.debug(f'using extra plugin path: "{path}"')

    plugin_paths.extend((x, 'extra') for x in extra_paths)

    # then core and stock come as the baseline
    plugin_paths.append((stock_plugins_path, 'stock'))
    plugin_paths.append((core_plugins_path, 'core'))

    return plugin_paths


async def main_async(config: SchedulerConfigProviderBase):
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

    scheduler = Scheduler(
        scheduler_config_provider=config,
        node_data_provider=PluginNodeDataProvider(
            plugin_paths=__construct_plugin_paths(
                custom_plugins_path=config.node_data_provider_custom_plugins_path(),
                plugin_search_locations=config.node_data_provider_extra_plugin_paths(),
            ),
        ),
        node_serializers=[NodeSerializerV2(), NodeSerializerV1()],
    )

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
    create_default_user_config_file('scheduler', SchedulerConfigProviderFileOverrides.generate_default_config_text())

    global_logger = logging.get_logger('scheduler')

    db_path = opts.db_path
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

    config = SchedulerConfigProviderFileOverrides(
        main_config=get_config('scheduler'),
        nodes_config=get_config('scheduler.nodes'),
        main_db_location=db_path,
        do_broadcast=opts.broadcast_interval > 0 if opts.broadcast_interval is not None else None,
        broadcast_interval=opts.broadcast_interval,
    )

    if opts.verbosity_pinger:
        logging.get_logger('scheduler.worker_pinger').setLevel(opts.verbosity_pinger)
    try:
        asyncio.run(main_async(config))
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
