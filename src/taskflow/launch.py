import sys
import argparse
from . import config
from . import logging


def main(argv):
    parser = argparse.ArgumentParser('taskflow', description='task execution thingie')
    parser.add_argument('--loglevel', help='logging level, like DEBUG, INFO, WARNING, ERROR')

    subparsers = parser.add_subparsers(title='command', required=True, dest='command')
    schedparser = subparsers.add_parser('scheduler', description='run main scheduler server')
    schedparser.add_argument('--db_path', help='path to sqlite database to use')

    workerparser = subparsers.add_parser('worker', description='run a worker')

    viewparser = subparsers.add_parser('viewer', description='run default viewer')

    opts = parser.parse_args(argv)

    if opts.command == 'scheduler':
        from .scheduler import main
        if opts.loglevel is not None:
            logging.set_default_loglevel(opts.loglevel)
        overrides = {}
        if opts.db_path:
            if 'scheduler' not in overrides:
                overrides['scheduler'] = {}
            overrides['scheduler']['db_path'] = opts.db_path

        if overrides:
            config.set_config_overrides('scheduler', overrides)
        return main()
    elif opts.command == 'worker':
        from .worker import main
        if opts.loglevel is not None:
            logging.set_default_loglevel(opts.loglevel)
        return main()
    elif opts.command == 'viewer':
        from .viewer.launch import main
        if opts.loglevel is not None:
            logging.set_default_loglevel(opts.loglevel)
        return main()


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
