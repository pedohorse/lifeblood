import sys
import argparse

from . import simple_worker_pool


def main(argv):
    parser = argparse.ArgumentParser('lifeblood pool')
    mut = parser.add_mutually_exclusive_group()
    mut.add_argument('--list', action='store_true', help='list availabele pool types')
    mut.add_argument('type', nargs='?', help='worker pool type to start')

    # a bit cheaty, but here we always have fixed amount of args
    opts = parser.parse_args(argv[:1])
    remaining_args = argv[1:]

    known_types = {'simple': simple_worker_pool}
    if opts.list:
        print('known pool types:\n' + '\n'.join(f'\t{x}' for x in known_types))
        return

    # for now it's hardcoded logic, in future TODO: make worker pools extendable as plugins
    if opts.type is None:
        print('no pool type provided!')
        parser.print_help()
        return 2
    if opts.type not in known_types:
        print(f'unknown pool type "{opts.type}"\nuse --list flag to list known pool types', file=sys.stderr)
        return 2

    known_types[opts.type].main(remaining_args)
