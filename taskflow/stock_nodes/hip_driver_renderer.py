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
        if any(x not in attrs for x in ('hipfile', 'hipdriver', 'frames')):
            return ProcessingResult()
        hippath = attrs['hipfile']
        driverpath = attrs['hipdriver']
        frames = attrs['frames']

        env = InvocationEnvironment()

        spawnlines = \
            "    kwargs = {frames=[frame]}\n" \
            "    if node.parm('filename'):\n" \
            "        kwargs['file'] = node.evalParm('filename')\n" \
            "    if node.parm('sopoutput'):\n" \
            "        kwargs['file'] = node.evalParm('sopoutput')\n" \
            "    taskflow_connection.create_task(node.name() + '_spawned frame %g' % frame, frames=[frame], ifdpath=)\n"

        if not self.is_output_connected('spawned'):
            spawnlines = ''

        script = \
            f'import hou\n' \
            f'import taskflow_connection\n' \
            f'print("opening file" + {repr(hippath)})\n' \
            f'hou.hipFile.load("{hippath}")\n' \
            f'node = hou.node("{driverpath}")\n' \
            f'for frame in {repr(frames)}:\n' \
            f'    print("rendering frame %d" % frame)\n' \
            f'    node.render(frame_range=(frame, frame))\n' \
            f'{spawnlines}' \
            f'print("all done!")\n'

        inv = InvocationJob(['hython', '-c', script], env)
        res = ProcessingResult(job=inv)
        return res

    def postprocess_task(self, task_dict) -> ProcessingResult:
        return ProcessingResult()
