from lifeblood.basenode import ProcessingResult, ProcessingContext, ProcessingError
from lifeblood.invocationjob import InvocationJob
from lifeblood.enums import NodeParameterType
from lifeblood_alicevision_modules.base_node import AlicevisionBaseNode
from pathlib import Path
from typing import Iterable


def node_class():
    return AlicevisionDepthMapFilter


class AlicevisionDepthMapFilter(AlicevisionBaseNode):
    def __init__(self, name):
        super(AlicevisionDepthMapFilter, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('input', 'Structure From Motion', NodeParameterType.STRING, "`task['av_structure_from_motion']`")
            ui.add_parameter('depthMapsFolder', 'Depth Maps', NodeParameterType.STRING, "`task['av_depth_map']`")
            ui.add_parameter('output', 'Output Filtered Depth Map Folder', NodeParameterType.STRING, "`config['global_scratch_location']`/alicevision/`task['uuid']`/DepthMapFilter")
            ui.add_separator()
            ui.add_parameter('minNumOfConsistentCams', 'Min Consistent Cams', NodeParameterType.INT, 3)
            ui.add_parameter('minNumOfConsistentCamsWithLowSimilarity', 'Min Consistent Cams Bad Similarity', NodeParameterType.INT, 4)

            ui.add_separator()
            with ui.parameters_on_same_line_block():
                ui.add_parameter('rangeStart', 'Image Range Start', NodeParameterType.INT, 0)
                ui.add_parameter('rangeSize', 'Image Chunk Size', NodeParameterType.INT, 999999)

    @classmethod
    def label(cls) -> str:
        return 'AV Depth Map Filter'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'alicevision', 'depth', 'map', 'filter'

    @classmethod
    def type_name(cls) -> str:
        return 'stock_alicevision_depth_map_filter'

    @classmethod
    def description(cls) -> str:
        return 'this node type does not have a description'

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        output = Path(context.param_value('output').strip())
        if not output.is_absolute():
            raise ProcessingError('output path must be absolute and not empty')

        output.mkdir(parents=True, exist_ok=True)

        args = ['aliceVision_depthMapFiltering', '--verboseLevel', 'info']
        args += ['--input', context.param_value('input').strip()]
        args += ['--depthMapsFolder', context.param_value('depthMapsFolder').strip()]
        args += ['--minNumOfConsistentCams', context.param_value('minNumOfConsistentCams')]
        args += ['--minNumOfConsistentCamsWithLowSimilarity', context.param_value('minNumOfConsistentCamsWithLowSimilarity')]
        for param_name in ('rangeStart', 'rangeSize'):
            args += [f'--{param_name}', context.param_value(param_name)]
        args += ['--output', output]

        job = InvocationJob([str(x) for x in args])
        res = ProcessingResult(job)
        res.set_attribute('av_depth_map_filtered', str(output))
        return res
