import time

from taskflow.basenode import BaseNode
from taskflow.invocationjob import InvocationJob, InvocationEnvironment
from taskflow.nodethings import ProcessingResult

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from taskflow.scheduler import Scheduler


def create_node_object(name: str):
    return Test(name)


class Test(BaseNode):
    def process_task(self, task_dict) -> ProcessingResult:
        td = InvocationJob(['bash', '-c',
                            'echo "startin..."\n'
                            'for i in {1..15}\n'
                            'do\n'
                            '    echo "iteration $i"\n'
                            '    echo $(date)\n'
                            '    sleep 1\n'
                            'done\n'
                            'echo "ended"\n'],
                           InvocationEnvironment())
        time.sleep(6)  # IMITATE LAUNCHING LONG BLOCKING OPERATION
        return ProcessingResult(job=td)

    def postprocess_task(self, task_dict) -> ProcessingResult:
        time.sleep(3.5)  # IMITATE LAUNCHING LONG BLOCKING OPERATION
        res = ProcessingResult()
        res.set_attribute('cat', 1)
        res.set_attribute('dog', 2)
        return res
