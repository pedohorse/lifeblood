import sys
import os
from pathlib import Path
import re
import asyncio
import lifeblood_connection
import json
import shutil
import tempfile

class ControlSignalProcessor:
    def __init__(self, my_addressee: str):
        self.__my_addressee = my_addressee
        self.__stop_event = asyncio.Event()
        self.__main_task = None

    def stop(self):
        self.__stop_event.set()

    async def wait_till_stops(self):
        if self.__main_task is None:
            raise RuntimeError('not started')
        await self.__main_task

    def start(self):
        self.__main_task = asyncio.create_task(self.__control_signal_processor())

    async def __control_signal_processor(self):
        control_waiter = asyncio.get_event_loop().run_in_executor(None, lifeblood_connection._message_to_invocation_receive_blocking, self.__my_addressee)
        stop_waiter = asyncio.create_task(self.__stop_event.wait())

        while True:
            done, pend = await asyncio.wait([control_waiter, stop_waiter], return_when=asyncio.FIRST_COMPLETED)
            if control_waiter in done:
                src, message = await control_waiter
                print(f'got control message: "{message.decode("latin1")}" from iid:{src}')
                if message == b'stop':
                    break
                control_waiter = asyncio.get_event_loop().run_in_executor(None, lifeblood_connection._message_to_invocation_receive_blocking, self.__my_addressee)
            if stop_waiter in done:
                break

        if not control_waiter.done():
            control_waiter.cancel()
        if not stop_waiter.done():
            stop_waiter.cancel()


async def main(port: int, webport: int, my_addressee: str, attr_file_path: str):
    # need to find houdini's python.
    # we cannot rely on hython being actual hython and not a wrapper
    # first we check obvious place, then actually call hython
    simtracker_path = None
    hfs_hou = Path(shutil.which('hython')).parent.parent / 'houdini'
    if hfs_hou.exists():
        for elem in hfs_hou.iterdir():
            if re.match(r'^python\d\.\d+libs$', elem.name) and (maybepath := elem / 'simtracker.py').exists():
                simtracker_path = str(maybepath)
                break

    if simtracker_path is None:
        fd, tpath = tempfile.mkstemp('.txt')
        tproc = await asyncio.create_subprocess_exec('hython', '-c', f'import simtracker;f=open({repr(tpath)},"w");f.write(simtracker.__file__);f.close()')
        tproc_exit_code = await tproc.wait()
        with open(tpath, 'r') as f:
            simtracker_path = f.read()
        os.close(fd)
        os.unlink(tpath)
        if tproc_exit_code != 0:
            print('FAILED to find simtracker')
            return tproc_exit_code

    signal_processor = ControlSignalProcessor(my_addressee)
    signal_processor.start()
    print('signal handler started')

    ip = lifeblood_connection.get_host_ip()
    # find free ports
    port = lifeblood_connection.get_free_tcp_port(ip, port)
    webport = lifeblood_connection.get_free_tcp_port(ip, webport)
    if webport == port:
        webport = lifeblood_connection.get_free_tcp_port(ip, webport + 1)
    print(f'found free ports: {port}, {webport}')

    # at this point we have free ports, but by the time we start our servers - ports might get taken
    # TODO: maybe add option to add '-v' flag for debugging
    proc = await asyncio.create_subprocess_exec('python', simtracker_path, str(port), str(webport))
    print('simtracker started')

    with open(attr_file_path, 'r') as f:
        attrs = json.load(f)
    attrs['simtracker_host'] = ip
    attrs['simtracker_port'] = port
    attrs['tracker_control_iid'] = lifeblood_connection.get_my_invocation_id()
    attrs['tracker_control_addressee'] = my_addressee
    lifeblood_connection.create_task('spawned task', attrs)

    proc_end_waiter = asyncio.create_task(proc.wait())
    signal_processor_waiter = asyncio.create_task(signal_processor.wait_till_stops())

    done, pend = await asyncio.wait([proc_end_waiter, signal_processor_waiter], return_when=asyncio.FIRST_COMPLETED)
    print(f'done, exiting!')
    for task in pend:
        task.cancel()

    proc_error = proc.returncode  # if simtracker process had error - return it, otherwise we terminate it and don't care about it's return code
    if proc_error is None:
        print(f'terminating process {proc.pid}')
        proc.terminate()
        await proc.wait()
        print('wait done')
    print('closed simtracker')
    signal_processor.stop()
    await signal_processor.wait_till_stops()
    print('closed server')

    return proc_error or 0  # read above - None counts as good as we terminate it ourselves


_port, _webport = [int(x) for x in sys.argv[1:3]]
sys.exit(asyncio.get_event_loop().run_until_complete(main(_port, _webport, sys.argv[3], sys.argv[4])))
