import sys
import subprocess
from pathlib import Path


def main():
    path_to_stuff = Path(__package__)/'stock_nodes'/'matrixclient'/'data'/'matrixclient.pyz'
    sys.exit(subprocess.Popen([sys.executable, path_to_stuff, *sys.argv[1:]]).wait())


if __name__ == '__main__':
    sys.exit(main() or 0)
