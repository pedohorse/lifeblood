#!/usr/bin/env python3

import os
import sys
import subprocess
from pathlib import Path


def main():
    cwd = Path(os.getcwd())
    mcdir = cwd/'matrixcommander'

    syspy = sys.executable

    if mcdir.exists():
        print('{mcdir} already exists! aborting install')
        return False

    print(f'installing matrix_commander venv into {mcdir}')
    mcdir.mkdir()

    # create venv
    res = subprocess.Popen([syspy, '-m', 'venv', 'venv'], cwd=mcdir).wait()
    if res != 0:
        print('error creating venv')
        return False
    venvpy = mcdir/'venv'/'bin'/'python'

    res = subprocess.Popen([venvpy, '-m', 'pip', 'install', 'matrix-commander~=6.0'], cwd=mcdir).wait()
    if res != 0:
        print('error installing matrix-commander into venv')
        return False

    mc_entry = mcdir/'venv'/'bin'/'matrix-commander'
    res = subprocess.Popen([venvpy, mc_entry, '--version'], cwd=mcdir).wait()
    if res == 0:
        print('success')
        return True
    else:
        print('something failed')
        return False


if __name__ == '__main__':
    sys.exit(0 if main() else 1)
