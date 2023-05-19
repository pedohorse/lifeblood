#!/bin/sh
"exec" "`dirname \`readlink -f $0\``/venv/bin/python" "`readlink -f $0`" "$@"
import sys
from lifeblood import launch

if __name__ == '__main__':
    sys.exit(launch.main(sys.argv[1:]))
