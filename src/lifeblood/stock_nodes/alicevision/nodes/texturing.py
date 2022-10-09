from lifeblood.basenode import ProcessingResult, ProcessingContext, ProcessingError
from lifeblood.invocationjob import InvocationJob
from lifeblood.enums import NodeParameterType
from lifeblood_alicevision_modules.base_node import AlicevisionBaseNode
from pathlib import Path
from typing import Iterable


def node_class():
    return AlicevisionTexturing


class AlicevisionTexturing(AlicevisionBaseNode):
    def __init__(self, name):
        super(AlicevisionTexturing, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('input', 'Dense SfM Data', NodeParameterType.STRING, "`task['av_dense_point_cloud']`")
            ui.add_parameter('imagesFolder', 'Images Folder', NodeParameterType.STRING, "`task['av_prepare_dense']`")
            ui.add_parameter('inputMesh', 'Input Mesh', NodeParameterType.STRING, "`task['av_mesh_object_filtered']`")
            ui.add_separator()
            ui.add_parameter('output', 'Output Dense Point Cloud', NodeParameterType.STRING, "`config['global_scratch_location']`/alicevision/`task['uuid']`/Texturing")
            ui.add_separator()
            ui.add_parameter('textureSide', 'Texture Side', NodeParameterType.INT, 8192)
            ui.add_parameter('downscale', 'Texture Downscale', NodeParameterType.INT, 2)
            ui.add_parameter('outputTextureFileType', 'Texture File Type', NodeParameterType.STRING, 'png')\
                .add_menu((('png', 'png'), ('jpg', 'jpg'), ('tiff', 'tiff'), ('exr', 'exr')))
            ui.add_parameter('unwrapMethod', 'Unwrap Method', NodeParameterType.STRING, 'Basic')\
                .add_menu((('Basic', 'Basic'), ('LSCM', 'LSCM'), ('ABF', 'ABF')))
            ui.add_parameter('useUDIM', 'Use UDIM', NodeParameterType.BOOL, True)
            ui.add_parameter('fillHoles', 'Fill Holes', NodeParameterType.BOOL, False)
            ui.add_parameter('correctEV', 'Correct Exposure', NodeParameterType.BOOL, False)

    @classmethod
    def label(cls) -> str:
        return 'AV Texturing'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'alicevision', 'texturing', 'mesh', 'texture'

    @classmethod
    def type_name(cls) -> str:
        return 'stock_alicevision_texturing'

    @classmethod
    def description(cls) -> str:
        return 'this node type does not have a description'

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        output = Path(context.param_value('output').strip())
        if not output.is_absolute():
            raise ProcessingError('output path must be absolute and not empty')

        output.mkdir(parents=True, exist_ok=True)

        args = ['aliceVision_texturing', '--verboseLevel', 'info']
        args += ['--input', context.param_value('input').strip()]
        args += ['--imagesFolder', context.param_value('imagesFolder').strip()]
        args += ['--inputMesh', context.param_value('inputMesh').strip()]
        for param_name in ('textureSide', 'downscale', 'outputTextureFileType',
                           'unwrapMethod', 'useUDIM', 'fillHoles', 'correctEV'):
            args += [f'--{param_name}', context.param_value(param_name)]

        args += ['--output', output]

        job = InvocationJob([str(x) for x in args])
        res = ProcessingResult(job)
        res.set_attribute('av_output_textured_folder', str(output))
        res.set_attribute('av_output_textured_obj', str(output/'texturedMesh.obj'))
        res.set_attribute('av_output_textured_mtl', str(output/'texturedMesh.mtl'))
        return res
