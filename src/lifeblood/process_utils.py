import os
import asyncio
import platform
import subprocess
import signal
import psutil

from .logging import get_logger

__logger = get_logger('process management')

oh_no_its_windows = platform.system() == 'Windows'


async def create_process(args, env) -> asyncio.subprocess.Process:
    if oh_no_its_windows:
        return await asyncio.create_subprocess_exec(
            *args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP  # this is cuz win "bReAk eVeNt" can only be sent to process group
        )
    else:
        return await asyncio.create_subprocess_exec(
            *args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            restore_signals=True,
            start_new_session=True
        )


async def kill_process_tree_posix(process: asyncio.subprocess.Process) -> int:
    try:
        puproc = psutil.Process(process.pid)
        all_proc = puproc.children(recursive=True)  # both these lines can raise NoSuchProcess
    except psutil.NoSuchProcess:
        __logger.warning(f'cannot find process with pid {process.pid}. Assuming it finished. retcode={process.returncode}')
    else:
        all_proc.append(puproc)
        for proc in all_proc:
            try:
                proc.terminate()
            except psutil.NoSuchProcess:
                pass
        for i in range(20):  # TODO: make a parameter out of this!
            if not all(not proc.is_running() for proc in all_proc):
                await asyncio.sleep(0.5)
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
        os.killpg(process.pid, signal.SIGKILL)
    except OSError:
        pass
    return await process.wait()


async def kill_process_tree_windows(process: asyncio.subprocess.Process) -> int:
    # this HOPEFULLY kills them
    process.send_signal(signal.CTRL_BREAK_EVENT)

    return await process.wait()
