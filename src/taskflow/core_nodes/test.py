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
            ui.add_parameter('test readonly crap', 'lelabel', NodeParameterType.STRING, 'wow im text', readonly=True)
            with ui.parameters_on_same_line_block():
                ui.add_parameter('text1', 'lobel', NodeParameterType.STRING, 'hi text i\'m dad', readonly=True)
                ui.add_parameter('text2', 'lebel', NodeParameterType.FLOAT, 4.22, readonly=True)
                ui.add_parameter('text3', 'lubel', NodeParameterType.INT, 1, readonly=True).add_menu([('fee', 0), ('foo', 1), ('faa', 2)])
                ui.add_parameter('text4', 'ha, and i\'m editable', NodeParameterType.STRING, 'yes')
            ui.add_parameter('some int or smth', 'into', NodeParameterType.INT, -1)

    @classmethod
    def label(cls) -> str:
        return 'test node'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'test', 'debug'

    @classmethod
    def type_name(cls) -> str:
        return 'test'

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
