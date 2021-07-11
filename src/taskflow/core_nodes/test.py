import time

from taskflow.basenode import BaseNode
from taskflow.invocationjob import InvocationJob, InvocationEnvironment
from taskflow.nodethings import ProcessingResult
from taskflow.uidata import NodeParameterType

from typing import TYPE_CHECKING, Iterable
if TYPE_CHECKING:
    from taskflow.scheduler import Scheduler


def node_class():
    return Test


class Test(BaseNode):
    def __init__(self, name):
        super(Test, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('multiline text', 'wow multi line', NodeParameterType.STRING, 'bla').set_text_multiline()
            ui.add_parameter('multiline python text', 'wow multi line code', NodeParameterType.STRING, '# shize').set_text_multiline('python')

    @classmethod
    def label(cls) -> str:
        return 'test node'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'test', 'debug'

    def process_task(self, context) -> ProcessingResult:
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
        if int(time.time()) % 10 == 0:
            raise RuntimeError('just happened to be in a wrong time in a wrong place')
        return ProcessingResult(job=td)

    def postprocess_task(self, context) -> ProcessingResult:
        time.sleep(3.5)  # IMITATE LAUNCHING LONG BLOCKING OPERATION
        res = ProcessingResult()
        res.set_attribute('cat', 1)
        res.set_attribute('dog', 2)
        if int(time.time()) % 10 == 0:
            raise RuntimeError('ain\'t ur lucky day, kid')
        return res
