import sys
import argparse
from . import logging


def main(argv):
    parser = argparse.ArgumentParser('lifeblood', description='task execution thingie')
    parser.add_argument('--loglevel', help='logging level, like DEBUG, INFO, WARNING, ERROR')

    subparsers = parser.add_subparsers(title='command', required=True, dest='command')
    schedparser = subparsers.add_parser('scheduler', description='run main scheduler server')

    workerparser = subparsers.add_parser('worker', description='run a worker')
    workerparser.add_argument('args', nargs=argparse.REMAINDER, help='arguments to pass to the worker')

    viewparser = subparsers.add_parser('viewer', description='run default viewer')

    baseopts, _ = parser.parse_known_args()
    cmd_id = argv.index(baseopts.command)
    cmd_argv = argv[cmd_id + 1:]
    baseargv = argv[:cmd_id + 1]
    opts = parser.parse_args(baseargv)

    if opts.command == 'scheduler':
        if opts.loglevel is not None:
            logging.set_default_loglevel(opts.loglevel)
        from .scheduler import main
        return main(cmd_argv)
    elif opts.command == 'worker':
        if opts.loglevel is not None:
            logging.set_default_loglevel(opts.loglevel)
        from .worker import main
        return main(cmd_argv)  # TODO: unify this approach to other commands
    elif opts.command == 'viewer':
        if opts.loglevel is not None:
            logging.set_default_loglevel(opts.loglevel)
        try:
            from lifeblood_viewer.launch import main
        except ImportError:
            logging.get_logger('main').error('Viewer python package not found. In needs be installed separately with smth like "pip install lifeblood_viewer"')
        return main(cmd_argv)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
