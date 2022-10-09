from lifeblood.basenode import ProcessingResult, ProcessingContext, ProcessingError
from lifeblood.invocationjob import InvocationJob
from lifeblood.enums import NodeParameterType
from lifeblood_alicevision_modules.base_node import AlicevisionBaseNode
from pathlib import Path
from typing import Iterable


def node_class():
    return AlicevisionPrepareDenseScene


class AlicevisionPrepareDenseScene(AlicevisionBaseNode):
    def __init__(self, name):
        super(AlicevisionPrepareDenseScene, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('input', 'Structure From Motion', NodeParameterType.STRING, "`task['av_structure_from_motion']`")
            ui.add_parameter('output', 'Output Image Folder', NodeParameterType.STRING, "`config['global_scratch_location']`/alicevision/`task['uuid']`/PrepareDenseScene")
            # ui.add_parameter('', 'Output Undistorted Images', NodeParameterType.STRING, "`config['global_scratch_location']`/alicevision/`task['uuid']`/StructureFromMotion/sfm.abc")

            ui.add_separator()
            with ui.parameters_on_same_line_block():
                ui.add_parameter('rangeStart', 'Image Range Start', NodeParameterType.INT, 0)
                ui.add_parameter('rangeSize', 'Image Chunk Size', NodeParameterType.INT, 999999)

    @classmethod
    def label(cls) -> str:
        return 'AV Prepare Dense Scene'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'alicevision', 'dense', 'scene', 'prepare'

    @classmethod
    def type_name(cls) -> str:
        return 'stock_alicevision_prepare_dense_scene'

    @classmethod
    def description(cls) -> str:
        return 'this node type does not have a description'

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        output = Path(context.param_value('output').strip())
        if not output.is_absolute():
            raise ProcessingError('output path must be absolute and not empty')

        output.mkdir(parents=True, exist_ok=True)

        args = ['aliceVision_prepareDenseScene', '--verboseLevel', 'info']
        args += ['--input', context.param_value('input').strip()]
        for param_name in ('rangeStart', 'rangeSize'):
            args += [f'--{param_name}', context.param_value(param_name)]
        args += ['--output', output]

        job = InvocationJob([str(x) for x in args])
        res = ProcessingResult(job)
        res.set_attribute('av_prepare_dense', str(output))
        return res
