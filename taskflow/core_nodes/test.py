import time

from taskflow.basenode import BaseNode
from taskflow.invocationjob import InvocationJob
from taskflow.taskspawn import TaskSpawn


def create_node_object(name: str):
    return Test()


def deserialize(data: bytes):
    return Test()


class Test(BaseNode):
    def process_task(self, task_dict):
        td = InvocationJob(['bash', '-c',
                            'echo "startin..."\n'
                            'for i in {1..60}\n'
                            'do\n'
                            '    echo "iteration $i"\n'
                            '    echo $(date)\n'
                            '    sleep 1\n'
                            'done\n'
                            'echo "ended"\n'],
                           None)
        time.sleep(6)  # IMITATE LAUNCHING LONG BLOCKING OPERATION
        return td, None

    def postprocess_task(self, task_dict):
        time.sleep(3.5)  # IMITATE LAUNCHING LONG BLOCKING OPERATION
        return {'cat': 1, 'dog': 2}, None

    def serialize(self):
        return b'_woop_'
