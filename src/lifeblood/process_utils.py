import sys
import os
import asyncio
import platform
import subprocess
import signal
import psutil

from .logging import get_logger

__logger = get_logger('process management')

oh_no_its_windows = platform.system() == 'Windows'

if oh_no_its_windows:
    import random
    import string
    import tempfile
    from win32job import CreateJobObject, TerminateJobObject

from typing import Optional


async def create_worker_process(args):
    """
    helper function to use to create a worker from worker pool

    :param args:
    :return:
    """
    if oh_no_its_windows:
        return await asyncio.create_subprocess_exec(*args, close_fds=True,
                                                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)  # this is cuz win "bReAk eVeNt" can only be sent to process group)
    else:
        return await asyncio.create_subprocess_exec(*args, close_fds=True)


def send_stop_signal_to_worker(process):
    """
    helper function to tell worker process to stop
    used by worker pool

    :param process:
    :return:
    """
    if oh_no_its_windows:
        return process.send_signal(signal.CTRL_BREAK_EVENT)
    else:
        return process.send_signal(signal.SIGTERM)


async def create_process(args: list, env: dict, cwd: Optional[str] = None, stdout=subprocess.PIPE, stderr=subprocess.PIPE) -> asyncio.subprocess.Process:
    """
    helper function mainly for worker to spawn a new process with a new process group.
    NOTE: process is created with stdout, stderr set to PIPE by default! careful not to deadlock!

    all arguments are passed (almost) directly to Popen

    :param args: arguments to run
    :param env: dict of environment variables
    :param cwd: current working directory to be set for the new process
    :param stdout: same as in Popen
    :param stderr: same as in Popen
    :return:
    """
    if oh_no_its_windows:
        class _HandleWrapper:
            """
            helper class that will delete temporary file when object is GCed
            """
            def __init__(self, fd, path):
                self.__fd = fd
                self.__path = path
                self.__logger = get_logger('win32_handle_wrapper')

            def __del__(self):
                try:
                    os.close(self.__fd)
                    os.unlink(self.__path)
                except Exception:
                    self.__logger.warning(f'failed to cleanup wrapped handle to: {self.__path}')

        wrapper_code = '''
import sys
import subprocess
from win32job import AssignProcessToJobObject, CreateJobObject 
from win32api import GetCurrentProcess

job = CreateJobObject(None, {job_name})
AssignProcessToJobObject(job, GetCurrentProcess())
sys.exit(subprocess.Popen({args}, env={env}, cwd={cwd}).wait())
'''
        job_name = ''.join(random.choice(string.ascii_letters) for _ in range(64))  # we HOPE there's no name collision
        job = CreateJobObject(None, job_name)

        # wrapper code may be too big for windows's ARGMAX limitations... so we'd better save it to a file
        fd, filepath = tempfile.mkstemp('_lnchr.py')
        with open(filepath, 'w') as f:
            f.write(wrapper_code.format(job_name=repr(job_name), args=repr(args), env=repr(env), cwd=repr(cwd)))

        proc = await asyncio.create_subprocess_exec(
            sys.executable, filepath,
            stdout=stdout,
            stderr=stderr,
            cwd=cwd,
            env=None,  # we need to execute it in our env, so that win32job module is available there
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP  # this is cuz win "bReAk eVeNt" can only be sent to process group
        )
        proc._win32_job_object = job
        proc._win32_handlewrapper = _HandleWrapper(fd, filepath)  # now when process object is deleted - temp file will eventually be cleaned by gc

        return proc
    else:
        return await asyncio.create_subprocess_exec(
            *args,
            stdout=stdout,
            stderr=stderr,
            cwd=cwd,
            env=env,
            restore_signals=True,
            start_new_session=True
        )


async def kill_process_tree(process: asyncio.subprocess.Process, graceful_close_timeout=10) -> int:
    """
    kill somehow

    :param process:
    :param graceful_close_timeout:
    :return:
    """
    if oh_no_its_windows:
        return await kill_process_tree_windows(process, graceful_close_timeout)
    return await kill_process_tree_posix(process, graceful_close_timeout)


async def kill_process_tree_posix(process: asyncio.subprocess.Process, graceful_close_timeout=10) -> int:
    """
    POSIX ONLY
    this one tries to walk the process tree and SIGTERM all processes, then SIGKILL ones that are stuck
    And then SIGKILL by the process group.
    Note: process set defined by pgroup and by process tree may easily be different.

    NOTE: this ASSUMES the process being killed IS the process group leader, as created by create_process

    :param process:
    :param graceful_close_timeout:
    :return:
    """
    all_proc = []
    try:
        puproc = psutil.Process(process.pid)
        all_proc.append(puproc)
        all_proc += puproc.children(recursive=True)  # both these lines can raise NoSuchProcess
    except psutil.NoSuchProcess:
        __logger.warning(f'cannot find process with pid {process.pid}. Assuming it finished. retcode={process.returncode}')
    else:
        for proc in all_proc:
            try:
                proc.terminate()
            except psutil.NoSuchProcess:
                pass
        poll_time = 0.5
        for i in range(int(graceful_close_timeout / poll_time)):
            if not all(not proc.is_running() for proc in all_proc):
                await asyncio.sleep(poll_time)
            else:
                break
        else:
            for proc in all_proc:
                if not proc.is_running():
                    continue
                try:
                    proc.kill()
                except psutil.NoSuchProcess:
                    pass

    # just to finish off all stray processes
    try:
        __logger.debug(f'sending SIGKILL to process group {process.pid}')
        os.killpg(process.pid, signal.SIGKILL)
    except OSError:
        __logger.warning('failed to send SIGKILL to group')
        pass
    return await process.wait()


async def kill_process_tree_windows(process: asyncio.subprocess.Process, graceful_close_timeout=10) -> int:
    """
    WINDOWS ONLY!

    :param process:
    :param graceful_close_timeout:
    :return:
    """
    # smth to read: https://stackoverflow.com/questions/35772001/how-to-handle-a-signal-sigint-on-a-windows-os-machine
    # first get all existing processes
    all_proc = []
    try:
        puproc = psutil.Process(process.pid)
        all_proc.append(puproc)
        all_proc += puproc.children(recursive=True)  # both these lines can raise NoSuchProcess
    except psutil.NoSuchProcess:
        __logger.warning(f'cannot find process with pid {process.pid}. Assuming it finished. retcode={process.returncode}')

    if hasattr(process, '_win32_job_object'):
        __logger.debug('terminating job object')
        TerminateJobObject(process._win32_job_object, 1)
        process._win32_job_object.close()
    else:
        __logger.warning('Failed to terminate the Job object. Trying to send BREAK event...')
        # this will actually act as SIGTERM to a pg, so here processes will have time to respond
        try:
            process.send_signal(signal.CTRL_BREAK_EVENT)
        except psutil.NoSuchProcess:
            __logger.warning('Failed to send BREAK event to process group! there might be wild processes still running!')

    # give them time to gracefully close
    poll_time = 0.5
    for i in range(int(graceful_close_timeout / poll_time)):
        if any(proc.is_running() for proc in all_proc):
            await asyncio.sleep(poll_time)
        else:
            break
    # now go through processes who did not stop and kill
    for proc in all_proc:
        try:
            proc.kill()
        except psutil.NoSuchProcess:
            pass

    return await process.wait()
