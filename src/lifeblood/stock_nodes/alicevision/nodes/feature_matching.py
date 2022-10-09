from lifeblood.basenode import ProcessingResult, ProcessingContext, ProcessingError
from lifeblood.invocationjob import InvocationJob
from lifeblood.enums import NodeParameterType
from lifeblood_alicevision_modules.base_node import AlicevisionBaseNode
from pathlib import Path
from typing import Iterable


def node_class():
    return AlicevisionFeatureMatching


class AlicevisionFeatureMatching(AlicevisionBaseNode):
    def __init__(self, name):
        super(AlicevisionFeatureMatching, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('input', 'Input Images Directory', NodeParameterType.STRING, "`task['av_camera_sfm']`")
            ui.add_parameter('featuresFolders', 'Features Dir', NodeParameterType.STRING, "`task['av_features_dir']`")  # TODO: this seem to be an array of dirs, so add support
            ui.add_parameter('imagePairsList', 'Image Pairs List', NodeParameterType.STRING, "`task['av_image_matches']`")
            ui.add_parameter('output', 'Output Feature Matching Dir', NodeParameterType.STRING, "`config['global_scratch_location']`/alicevision/`task['uuid']`/FeatureMatching")
            ui.add_separator()
            ui.add_parameter('describerTypes', 'Describer Types', NodeParameterType.STRING, 'sift')  # TODO: make a set of checkboxes
            ui.add_parameter('crossMatching', 'Cross Matching', NodeParameterType.BOOL, False)
            ui.add_parameter('guidedMatching', 'Guided Matching', NodeParameterType.BOOL, False)
            ui.add_parameter('matchFromKnownCameraPoses', 'Match From Known Camera Poses', NodeParameterType.BOOL, False)
            ui.add_separator()
            ui.add_parameter('distanceRatio', 'distanceRatio', NodeParameterType.FLOAT, 0.8)
            ui.add_parameter('maxIteration', 'maxIteration', NodeParameterType.INT, 2048)

            ui.add_separator()
            with ui.parameters_on_same_line_block():
                ui.add_parameter('rangeStart', 'Image Range Start', NodeParameterType.INT, 0)
                ui.add_parameter('rangeSize', 'Image Chunk Size', NodeParameterType.INT, 999999)

    @classmethod
    def label(cls) -> str:
        return 'AV Feature Matching'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'alicevision', 'feature', 'matching'

    @classmethod
    def type_name(cls) -> str:
        return 'stock_alicevision_feature_matching'

    @classmethod
    def description(cls) -> str:
        return 'this node type does not have a description'

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        output = Path(context.param_value('output').strip())
        if not output.is_absolute():
            raise ProcessingError('output path must be absolute and not empty')

        output.mkdir(parents=True, exist_ok=True)  # TODO: move this to worker

        args = ['aliceVision_featureMatching', '--verboseLevel', 'info']
        for param_name in ('input', 'featuresFolders', 'imagePairsList',
                           'describerTypes', 'crossMatching', 'guidedMatching', 'matchFromKnownCameraPoses',
                           'distanceRatio', 'maxIteration',
                           'rangeStart', 'rangeSize'):
            args += [f'--{param_name}', context.param_value(param_name)]
        args += ['--output', output]

        job = InvocationJob([str(x) for x in args])
        res = ProcessingResult(job)
        res.set_attribute('av_feature_matches', str(output))
        return res
