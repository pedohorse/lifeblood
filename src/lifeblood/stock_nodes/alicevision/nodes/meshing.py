from lifeblood.basenode import ProcessingResult, ProcessingContext, ProcessingError
from lifeblood.invocationjob import InvocationJob
from lifeblood.enums import NodeParameterType
from lifeblood_alicevision_modules.base_node import AlicevisionBaseNode
from pathlib import Path
from typing import Iterable


def node_class():
    return AlicevisionMeshing


class AlicevisionMeshing(AlicevisionBaseNode):
    def __init__(self, name):
        super(AlicevisionMeshing, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('input', 'Structure From Motion', NodeParameterType.STRING, "`task['av_structure_from_motion']`")
            ui.add_parameter('depthMapsFolder', 'Depth Maps', NodeParameterType.STRING, "`task['av_depth_map_filtered']`")
            ui.add_parameter('outputMesh', 'Output Mesh', NodeParameterType.STRING, "`config['global_scratch_location']`/alicevision/`task['uuid']`/Meshing/mesh.obj")
            ui.add_parameter('output', 'Output Dense Point Cloud', NodeParameterType.STRING, "`config['global_scratch_location']`/alicevision/`task['uuid']`/Meshing/densePointCloud.abc")
            ui.add_separator()
            # TODO: add bounding box parameter
            ui.add_parameter('estimateSpaceMinObservationAngle', 'Min Observations Angle For SfM Space Estimation', NodeParameterType.INT, 10)
            ui.add_parameter('maxInputPoints', 'Max Input Points', NodeParameterType.INT, 50_000_000)
            ui.add_parameter('maxPoints', 'Max Points', NodeParameterType.INT, 5_000_000)
            ui.add_parameter('voteFilteringForWeaklySupportedSurfaces', 'Weakly Supported Surface Support', NodeParameterType.BOOL, True)
            ui.add_parameter('colorizeOutput', 'Colorize Output', NodeParameterType.BOOL, False)

    @classmethod
    def label(cls) -> str:
        return 'AV Meshing'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'alicevision', 'meshing'

    @classmethod
    def type_name(cls) -> str:
        return 'stock_alicevision_meshing'

    @classmethod
    def description(cls) -> str:
        return 'this node type does not have a description'

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        output_mesh = Path(context.param_value('outputMesh').strip())
        if not output_mesh.is_absolute():
            raise ProcessingError('output mesh path must be absolute and not empty')
        output = Path(context.param_value('output').strip())
        if not output.is_absolute():
            raise ProcessingError('output path must be absolute and not empty')

        output.parent.mkdir(parents=True, exist_ok=True)
        output_mesh.parent.mkdir(parents=True, exist_ok=True)

        args = ['aliceVision_meshing', '--verboseLevel', 'info']
        args += ['--input', context.param_value('input').strip()]
        args += ['--depthMapsFolder', context.param_value('depthMapsFolder').strip()]
        for param_name in ('estimateSpaceMinObservationAngle', 'maxInputPoints', 'maxPoints',
                           'voteFilteringForWeaklySupportedSurfaces', 'colorizeOutput'):
            args += [f'--{param_name}', context.param_value(param_name)]

        args += ['--output', output]
        args += ['--outputMesh', output_mesh]

        job = InvocationJob([str(x) for x in args])
        res = ProcessingResult(job)
        res.set_attribute('av_mesh_object', str(output_mesh))
        res.set_attribute('av_dense_point_cloud', str(output))
        return res
