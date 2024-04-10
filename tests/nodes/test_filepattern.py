import shutil
from unittest import TestCase
from lifeblood_testing_common.nodes_common import plugin_data_provider
from contextlib import contextmanager
import tempfile
import shutil
import pathlib


@contextmanager
def temp_dir():
    dirpath = tempfile.mkdtemp('', 'lifeblood_test_')
    yield pathlib.Path(dirpath)
    shutil.rmtree(dirpath)


class TestScan(TestCase):

    def test_patterns(self):
        scan_func = plugin_data_provider.node_class('filepattern')._scan_one_level
        with temp_dir() as tmp:  # type: pathlib.Path
            (tmp / 'food').mkdir()
            (tmp / 'faad/shmaad').mkdir(parents=True)
            (tmp / 'bard').mkdir()
            (tmp / 'burd/shmurd').mkdir(parents=True)
            (tmp / 'foof').touch()
            (tmp / 'faaf').touch()
            (tmp / 'barf').touch()
            (tmp / 'burf').touch()

            base_pattern = list(tmp.parts)
            for pattern in (
                    base_pattern + ['*'],
                    base_pattern + ['', '*'],
                    base_pattern + ['', '', '*'],
            ):
                files = scan_func('', pattern, do_files=True, do_dirs=True)
                self.assertSetEqual({str(tmp / x) for x in ('foof', 'food', 'faaf', 'faad', 'barf', 'bard', 'burf', 'burd')}, set(files))
                files = scan_func('', pattern, do_files=False, do_dirs=True)
                self.assertSetEqual({str(tmp / x) for x in ('food', 'faad', 'bard', 'burd')}, set(files))
                files = scan_func('', pattern, do_files=True, do_dirs=False)
                self.assertSetEqual({str(tmp / x) for x in ('foof', 'faaf', 'barf', 'burf')}, set(files))
                files = scan_func('', pattern, do_files=False, do_dirs=False)
                self.assertSetEqual(set(), set(files))

            pattern = base_pattern + ['fo*']
            files = scan_func('', pattern, do_files=True, do_dirs=True)
            self.assertSetEqual({str(tmp / x) for x in ('foof', 'food')}, set(files))
            files = scan_func('', pattern, do_files=False, do_dirs=True)
            self.assertSetEqual({str(tmp / x) for x in ('food',)}, set(files))
            files = scan_func('', pattern, do_files=True, do_dirs=False)
            self.assertSetEqual({str(tmp / x) for x in ('foof',)}, set(files))
            files = scan_func('', pattern, do_files=False, do_dirs=False)
            self.assertSetEqual(set(), set(files))

            pattern = base_pattern + ['*d', 'shm*']
            files = scan_func('', pattern, do_files=True, do_dirs=True)
            self.assertSetEqual({str(tmp / x) for x in ('faad/shmaad', 'burd/shmurd')}, set(files))

