from lifeblood.basenode import ProcessingResult, ProcessingContext, ProcessingError
from lifeblood.invocationjob import InvocationJob
from lifeblood.enums import NodeParameterType
from lifeblood_alicevision_modules.base_node import AlicevisionBaseNode
from pathlib import Path
from typing import Iterable


def node_class():
    return AlicevisionDepthMap


class AlicevisionDepthMap(AlicevisionBaseNode):
    def __init__(self, name):
        super(AlicevisionDepthMap, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('input', 'Structure From Motion', NodeParameterType.STRING, "`task['av_structure_from_motion']`")
            ui.add_parameter('imagesFolder', 'Dense Images', NodeParameterType.STRING, "`task['av_prepare_dense']`")
            ui.add_parameter('output', 'Output Depth Map Folder', NodeParameterType.STRING, "`config['global_scratch_location']`/alicevision/`task['uuid']`/DepthMap")
            ui.add_separator()
            ui.add_parameter('downscale', 'Downscale', NodeParameterType.INT, 2)\
                .add_menu((('1', 1), ('2', 2), ('3', 3), ('4', 4)))
            ui.add_parameter('sgmMaxTCams', 'SGM: Nb Neighbour Cameras', NodeParameterType.INT, 10)
            ui.add_parameter('refineMaxTCams', 'Refine: Nb Neighbour Cameras', NodeParameterType.INT, 6)

            ui.add_separator()
            with ui.parameters_on_same_line_block():
                ui.add_parameter('rangeStart', 'Image Range Start', NodeParameterType.INT, 0)
                ui.add_parameter('rangeSize', 'Image Chunk Size', NodeParameterType.INT, 999999)

    @classmethod
    def label(cls) -> str:
        return 'AV Depth Map'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'alicevision', 'depth', 'map'

    @classmethod
    def type_name(cls) -> str:
        return 'stock_alicevision_depth_map'

    @classmethod
    def description(cls) -> str:
        return 'this node type does not have a description'

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        output = Path(context.param_value('output').strip())
        if not output.is_absolute():
            raise ProcessingError('output path must be absolute and not empty')

        output.mkdir(parents=True, exist_ok=True)

        args = ['aliceVision_depthMapEstimation', '--verboseLevel', 'info']
        args += ['--input', context.param_value('input').strip()]
        args += ['--imagesFolder', context.param_value('imagesFolder').strip()]
        args += ['--sgmMaxTCams', context.param_value('sgmMaxTCams')]
        args += ['--refineMaxTCams', context.param_value('refineMaxTCams')]
        for param_name in ('rangeStart', 'rangeSize'):
            args += [f'--{param_name}', context.param_value(param_name)]
        args += ['--output', output]

        job = InvocationJob([str(x) for x in args])
        res = ProcessingResult(job)
        res.set_attribute('av_depth_map', str(output))
        return res
