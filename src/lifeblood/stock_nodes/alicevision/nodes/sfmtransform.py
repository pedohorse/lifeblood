from lifeblood.basenode import ProcessingResult, ProcessingContext, ProcessingError
from lifeblood.invocationjob import InvocationJob
from lifeblood.enums import NodeParameterType
from lifeblood_alicevision_modules.base_node import AlicevisionBaseNode
from pathlib import Path
from typing import Iterable


def node_class():
    return AlicevisionSfMTransform


class AlicevisionSfMTransform(AlicevisionBaseNode):
    def __init__(self, name):
        super(AlicevisionSfMTransform, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('input', 'Structure From Motion', NodeParameterType.STRING, "`task['av_structure_from_motion']`")
            ui.add_separator()
            ui.add_parameter('output', 'Output Structure From Motion', NodeParameterType.STRING, "`config['global_scratch_location']`/alicevision/`task['uuid']`/SfMTransform/sfm.abc")
            ui.add_parameter('outputViewsAndPoses', 'Output Views And Poses', NodeParameterType.STRING, "`config['global_scratch_location']`/alicevision/`task['uuid']`/SfMTransform/cameras.sfm")
            ui.add_separator()
            # TODO: add modes: transformation, manual, from_single_camera
            ui.add_parameter('method', 'Transformation Method', NodeParameterType.STRING, 'auto_from_cameras')\
                .add_menu((('auto from cameras', 'auto_from_cameras'),
                           ('auto from landmarks', 'auto_from_landmarks'),
                           ('from center camera', 'from_center_camera'),
                           ('from markers', 'from_markers')))
            ui.add_parameter('landmarksDescriberTypes', 'Describer Types', NodeParameterType.STRING, 'sift,dspsift,akaze')  # TODO: make a set of checkboxes
            ui.add_parameter('scale', 'Additional Scale', NodeParameterType.FLOAT, 1.0)
            # TODO: missing markers parameter
            ui.add_parameter('applyScale', 'Apply Scale', NodeParameterType.BOOL, True)
            ui.add_parameter('applyRotation', 'Apply Rotation', NodeParameterType.BOOL, True)
            ui.add_parameter('applyTranslation', 'Apply Translation', NodeParameterType.BOOL, True)

    @classmethod
    def label(cls) -> str:
        return 'AV SfM Transform'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'alicevision', 'sfm', 'structure', 'motion', 'transform'

    @classmethod
    def type_name(cls) -> str:
        return 'stock_alicevision_utils_sfm_transform'

    @classmethod
    def description(cls) -> str:
        return 'this node type does not have a description'

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        output = Path(context.param_value('output').strip())
        if not output.is_absolute():
            raise ProcessingError('output path must be absolute and not empty')
        output_vap = Path(context.param_value('outputViewsAndPoses'))
        if not output_vap.is_absolute():
            raise ProcessingError('output path must be absolute and not empty')

        output.parent.mkdir(parents=True, exist_ok=True)
        output_vap.parent.mkdir(parents=True, exist_ok=True)

        args = ['aliceVision_utils_sfmTransform', '--verboseLevel', 'info']
        args += ['--input', context.param_value('input').strip()]

        for param_name in ('method', 'landmarksDescriberTypes', 'scale',
                           'applyScale', 'applyRotation', 'applyTranslation'):
            args += [f'--{param_name}', context.param_value(param_name)]

        args += ['--output', output]
        args += ['--outputViewsAndPoses', output_vap]

        job = InvocationJob([str(x) for x in args])
        res = ProcessingResult(job)
        res.set_attribute('av_structure_from_motion', str(output))
        res.set_attribute('av_viewes_and_poses', str(output_vap))
        return res
