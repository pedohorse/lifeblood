from lifeblood.basenode import ProcessingResult, ProcessingContext, ProcessingError
from lifeblood.invocationjob import InvocationJob
from lifeblood.enums import NodeParameterType
from lifeblood_alicevision_modules.base_node import AlicevisionBaseNode
from pathlib import Path
from typing import Iterable


def node_class():
    return AlicevisionMeshFiltering


class AlicevisionMeshFiltering(AlicevisionBaseNode):
    def __init__(self, name):
        super(AlicevisionMeshFiltering, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('inputMesh', 'Mesh', NodeParameterType.STRING, "`task['av_mesh_object']`")
            ui.add_parameter('outputMesh', 'Output Filtered Mesh', NodeParameterType.STRING, "`config['global_scratch_location']`/alicevision/`task['uuid']`/MeshFiltering/mesh.obj")
            ui.add_separator()
            ui.add_parameter('keepLargestMeshOnly', 'Keep Only the Largest Mesh', NodeParameterType.BOOL, False)
            ui.add_parameter('smoothingIterations', 'Smoothing Iterations', NodeParameterType.INT, 5)
            ui.add_parameter('filterLargeTrianglesFactor', 'Filter Large Triangles Factor', NodeParameterType.FLOAT, 60.0)

    @classmethod
    def label(cls) -> str:
        return 'AV Mesh Filtering'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'alicevision', 'meshing', 'filter'

    @classmethod
    def type_name(cls) -> str:
        return 'stock_alicevision_mesh_filtering'

    @classmethod
    def description(cls) -> str:
        return 'this node type does not have a description'

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        output = Path(context.param_value('outputMesh').strip())
        if not output.is_absolute():
            raise ProcessingError('output path must be absolute and not empty')

        output.parent.mkdir(parents=True, exist_ok=True)

        args = ['aliceVision_meshFiltering', '--verboseLevel', 'info']
        args += ['--inputMesh', context.param_value('inputMesh').strip()]
        for param_name in ('keepLargestMeshOnly', 'smoothingIterations', 'filterLargeTrianglesFactor'):
            args += [f'--{param_name}', context.param_value(param_name)]

        args += ['--outputMesh', output]

        job = InvocationJob([str(x) for x in args])
        res = ProcessingResult(job)
        res.set_attribute('av_mesh_object_filtered', str(output))
        return res
