import os
import tempfile
import asyncio
import aiofiles

try:
    # linux/macos
    import fcntl


    def lock_file(f):
        fcntl.lockf(f, fcntl.LOCK_EX)


    def unlock_file(f):
        fcntl.lockf(f, fcntl.LOCK_UN)

except ImportError:
    # wandowz

    import msvcrt

    def lock_file(f):
        while True:
            try:
                msvcrt.locking(f, msvcrt.LK_RLCK, 1)
                break
            except IOError:  # there is a hardcoded timeout of 10s for this method, however we either want our own timeout, or proper blocking
                continue


    def unlock_file(f):
        msvcrt.locking(f, msvcrt.LK_UNLCK, 1)


class FileLock:
    """
    this should ne a pretty robust solution, but it was not properly tested
    """

    def __init__(self, lockname, base_path=None):
        if base_path is None:
            base_path = os.path.join(tempfile.gettempdir(), 'lifeblood_locks')
        self.__lockdir = os.path.join(base_path, 'locks')
        self.__lockfile = os.path.join(self.__lockdir, lockname)
        if not os.path.exists(self.__lockdir):
            os.makedirs(self.__lockdir, exist_ok=True)
        self.__f = None

    def __enter__(self):
        self.__f = open(self.__lockfile, 'w')
        lock_file(self.__f.fileno())

    def __exit__(self, exc_type, exc_val, exc_tb):
        unlock_file(self.__f.fileno())
        self.__f.close()

    async def __aenter__(self):
        self.__f = await aiofiles.open(self.__lockfile, 'w')
        await asyncio.get_event_loop().run_in_executor(None, lock_file, self.__f.fileno())

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await asyncio.get_event_loop().run_in_executor(None, unlock_file, self.__f.fileno())
        await self.__f.close()


if __name__ == '__main__':
    import time

    def _test():
        def do_lock(n):
            print(f'proc {n} start')
            with FileLock('testing'):
                for i in range(11):
                    print(f'proc {n} in lock {i}')
                    time.sleep(0.9)

        # import threading
        #
        # for i in range(3):
        #     threading.Thread(target=do_lock, args=(i,)).start()

        # threads share locks, so test above^ won't work
        do_lock(0)  # just run this shit several times, this is not a proper unittest anyway


    _test()
