import sys
import argparse
from . import logging


def main(argv):
    parser = argparse.ArgumentParser('lifeblood', description='task execution thingie')
    parser.add_argument('--loglevel', help='logging level, like DEBUG, INFO, WARNING, ERROR')

    subparsers = parser.add_subparsers(title='command', required=True, dest='command')
    schedparser = subparsers.add_parser('scheduler', description='run main scheduler server', add_help=False)

    workerparser = subparsers.add_parser('worker', description='run a worker', add_help=False)
    workerparser.add_argument('args', nargs=argparse.REMAINDER, help='arguments to pass to the worker')

    viewparser = subparsers.add_parser('viewer', description='run default viewer')

    poolparser = subparsers.add_parser('pool', description='run a worker pool', add_help=False)
    poolparser.add_argument('args', nargs=argparse.REMAINDER, help='arguments to pass to the pool')

    envresparser = subparsers.add_parser('resolver', description='configurate environment resolvers', add_help=False)
    envresparser.add_argument('args', nargs=argparse.REMAINDER, help='arguments to pass to the pool')

    baseopts, _ = parser.parse_known_args(argv)
    cmd_id = argv.index(baseopts.command)
    cmd_argv = argv[cmd_id + 1:]
    baseargv = argv[:cmd_id + 1]
    opts = parser.parse_args(baseargv)

    if opts.loglevel is not None:
        logging.set_default_loglevel(opts.loglevel)

    if opts.command == 'scheduler':
        from .scheduler import main
        return main(cmd_argv)
    elif opts.command == 'worker':
        from .worker import main
        return main(cmd_argv)
    elif opts.command == 'pool':
        from .worker_pool import main
        return main(cmd_argv)
    elif opts.command == 'viewer':
        try:
            from lifeblood_viewer.launch import main
        except ImportError as e:
            logger = logging.get_logger('main')
            logger.error('Viewer python package not found. In needs be installed separately with smth like "pip install lifeblood_viewer"')
            logger.exception(e)
            return
        return main(cmd_argv)
    elif opts.command == 'resolver':
        from .environment_resolver import main
        return main(cmd_argv)


def console_entry_point():
    sys.exit(main(sys.argv[1:]))


if __name__ == '__main__':
    console_entry_point()
