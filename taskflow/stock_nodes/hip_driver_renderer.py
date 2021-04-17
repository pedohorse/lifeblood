from copy import copy
from taskflow.basenode import BaseNode
from taskflow.enums import NodeParameterType
from taskflow.nodethings import ProcessingResult, TaskSpawn
from taskflow.invocationjob import InvocationJob, InvocationEnvironment


def create_node_object(name: str):
    return HipDriverRenderer(name)


class HipDriverRenderer(BaseNode):
    def __init__(self, name):
        super(HipDriverRenderer, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_output_for_spawned_tasks()
            ui.add_parameter('driver path', 'rop node path', NodeParameterType.STRING, '')
            ui.add_parameter('override', 'override with hipdriver attribute', NodeParameterType.BOOL, False)

    def process_task(self, task_dict) -> ProcessingResult:
        """
        this node expects to find the following attributes:
        frames
        hipfile
        hipdriver
        :param task_dict:
        :return:
        """
        attrs = self.get_attributes(task_dict)
        if any(x not in attrs for x in ('hipfile', 'frames')):
            return ProcessingResult()
        hippath = attrs['hipfile']
        if self.param_value('override') and 'hipdriver' in attrs:
            driverpath = attrs['hipdriver']
        else:
            driverpath = self.param_value('driver path')
        frames = attrs['frames']

        env = InvocationEnvironment()

        spawnlines = \
            f"    kwargs = {{'frames':[frame], 'hipfile':'{hippath}'}}\n" \
            f"    if node.parm('filename'):\n" \
            f"        kwargs['file'] = node.evalParm('filename')\n" \
            f"    if node.parm('sopoutput'):\n" \
            f"        kwargs['file'] = node.evalParm('sopoutput')\n" \
            f"    taskflow_connection.create_task(node.name() + '_spawned frame %g' % frame, **kwargs)\n"

        if not self.is_output_connected('spawned'):
            spawnlines = ''

        script = \
            f'import hou\n' \
            f'import taskflow_connection\n' \
            f'print("opening file" + {repr(hippath)})\n' \
            f'hou.hipFile.load("{hippath}")\n' \
            f'node = hou.node("{driverpath}")\n' \
            f'for frame in {repr(frames)}:\n' \
            f'    hou.setFrame(frame)\n' \
            f'    print("rendering frame %d" % frame)\n' \
            f'    node.render(frame_range=(frame, frame))\n' \
            f'{spawnlines}' \
            f'print("all done!")\n'

        inv = InvocationJob(['hython', '-c', script], env)
        res = ProcessingResult(job=inv)
        return res

    def postprocess_task(self, task_dict) -> ProcessingResult:
        return ProcessingResult()
