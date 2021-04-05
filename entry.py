#!/usr/bin/env python
import sys
import os
import argparse


def main(argv):
    parser = argparse.ArgumentParser('taskflow', description='task execution thingie')

    subparsers = parser.add_subparsers(title='command', required=True, dest='command')
    schedparser = subparsers.add_parser('scheduler', description='run main scheduler server')

    workerparser = subparsers.add_parser('worker', description='run a worker')

    viewparser = subparsers.add_parser('viewer', description='run default viewer')

    opts = parser.parse_args(argv)

    if opts.command == 'scheduler':
        from taskflow.scheduler import main
        return main()
    elif opts.command == 'worker':
        from taskflow.worker import main
        return main()
    elif opts.command == 'viewer':
        from taskflow.viewer.launch import main
        return main()


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
