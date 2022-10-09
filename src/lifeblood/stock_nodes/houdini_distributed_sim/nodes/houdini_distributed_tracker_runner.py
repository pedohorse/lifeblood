import re
import inspect

from lifeblood.basenode import BaseNode
from lifeblood.nodethings import ProcessingResult, InvocationJob
from lifeblood.invocationjob import InvocationRequirements
from lifeblood.enums import NodeParameterType, WorkerType
from lifeblood.text import filter_by_pattern

from typing import Iterable


def node_class():
    return HoudiniDistributedTracker


async def do(port, webport, killport):
    import asyncio
    import os
    import signal
    import socket
    import lifeblood_connection
    import json

    close_event = asyncio.Event()

    async def conn_cb(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        await reader.readexactly(1)
        print('got termination request')
        writer.close()
        close_event.set()

    ip = lifeblood_connection.get_host_ip()
    # find free ports
    port = lifeblood_connection.get_free_tcp_port(ip, port)
    webport = lifeblood_connection.get_free_tcp_port(ip, webport)
    if webport == port:
        webport = lifeblood_connection.get_free_tcp_port(ip, webport + 1)
    killport = lifeblood_connection.get_free_tcp_port(ip, killport)
    if killport == port:
        killport = lifeblood_connection.get_free_tcp_port(ip, killport + 1)
    if killport == webport:  # NOTE: port < webport, always, so we need 2 independent checks. worst case both conditions may be true
        killport = lifeblood_connection.get_free_tcp_port(ip, killport + 1)

    # at this point we have free ports, but by the time we start our servers - ports might get taken

    server = await asyncio.start_server(conn_cb, port=killport, family=socket.AF_INET)
    proc = await asyncio.create_subprocess_exec('hython', '-m', 'simtracker', '-v', str(port), str(webport))

    async def server_close_waiter(server: asyncio.AbstractServer):
        await close_event.wait()
        server.close()
        await server.wait_closed()

    async def proc_end_waiter(proc: asyncio.subprocess.Process):
        return await proc.wait()

    await server.start_serving()

    attrs = json.loads(os.environ['LBATTRS_JSON'])
    attrs['simtracker_host'] = ip
    attrs['simtracker_port'] = port
    attrs['tracker kill port'] = killport
    lifeblood_connection.create_task('spawned task', attrs)
    await asyncio.wait([proc_end_waiter(proc), server_close_waiter(server)], return_when=asyncio.FIRST_COMPLETED)
    print(f'done, exiting!')

    proc_error = proc.returncode  # if simtracker process had error - return it, otherwise we terminate it and don't care about it's return code
    if proc_error is None:
        proc.terminate()
        await proc.wait()
        print('wait done')
    print('closed simtracker')
    if server.is_serving():
        server.close()
        await server.wait_closed()
    print('closed server')

    return proc_error or 0  # read above - None counts as good as we terminate it ourselves


class HoudiniDistributedTracker(BaseNode):
    @classmethod
    def label(cls) -> str:
        return 'houdini sim tracker'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'houdini', 'tracker', 'distributed'

    @classmethod
    def type_name(cls) -> str:
        return 'houdini_distributed_tracker'

    @classmethod
    def description(cls) -> str:
        return 'creates a running houdini distributed simulation tracker\n' \
               'when tracker is up and running - a child task is created\n' \
               'child task inherits all attributes from parent\n' \
               'additionally these attribs are created:\n' \
               '    SIMTRACKER_HOST: hostname of the tracker\n' \
               '    SIMTRACKER_PORT: port of the tracker\n' \
               '    tracker kill port: port of the tracker for control commands'

    def __init__(self, name: str):
        super(HoudiniDistributedTracker, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.5, 0.25, 0.125)
            ui.add_output('spawned')
            ui.add_parameter('port', 'sim port', NodeParameterType.INT, 19375)
            ui.add_parameter('wport', 'web port', NodeParameterType.INT, 19376)
            ui.add_parameter('kport', 'kill port', NodeParameterType.INT, 19377)
            ui.add_parameter('use helper', 'run on scheduler helper', NodeParameterType.BOOL, True)

    def process_task(self, context) -> ProcessingResult:
        code = 'import sys\n' \
               'import asyncio\n\n'
        code += inspect.getsource(do)
        code += '\n' \
                'port, webport, killport = [int(x) for x in sys.argv[1:4]]\n' \
                'sys.exit(asyncio.get_event_loop().run_until_complete(do(port, webport, killport)))\n'
        invoc = InvocationJob(['python', ':/work_to_do.py', context.param_value('port'), context.param_value('wport'), context.param_value('kport')])
        invoc.set_extra_file('work_to_do.py', code)

        if context.param_value('use helper'):
            invoc.set_requirements(InvocationRequirements(worker_type=WorkerType.SCHEDULER_HELPER))
        res = ProcessingResult(invoc)

        return res
