from lifeblood.basenode import ProcessingResult, ProcessingContext, ProcessingError
from lifeblood.invocationjob import InvocationJob
from lifeblood.enums import NodeParameterType
from lifeblood_alicevision_modules.base_node import AlicevisionBaseNode
from pathlib import Path
from typing import Iterable


def node_class():
    return AlicevisionFeatureExtraction


class AlicevisionFeatureExtraction(AlicevisionBaseNode):
    def __init__(self, name):
        super(AlicevisionFeatureExtraction, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('input', 'Input Images Directory', NodeParameterType.STRING, "`task['av_camera_sfm']`")
            ui.add_parameter('output', 'Output Feature Dir', NodeParameterType.STRING, "`config['global_scratch_location']`/alicevision/`task['uuid']`/FeatureExtraction")
            ui.add_separator()
            ui.add_parameter('describerTypes', 'Describer Types', NodeParameterType.STRING, 'sift')  # TODO: make a set of checkboxes
            ui.add_parameter('describerPreset', 'Describer Density', NodeParameterType.STRING, 'normal')\
                .add_menu((('low', 'low'), ('medium', 'medium'), ('normal', 'normal'), ('high', 'high'), ('ultra', 'ultra')))
            ui.add_parameter('describerQuality', 'Describer Quality', NodeParameterType.STRING, 'normal')\
                .add_menu((('low', 'low'), ('medium', 'medium'), ('normal', 'normal'), ('high', 'high'), ('ultra', 'ultra')))
            ui.add_parameter('forceCpuExtraction', 'Force Cpu', NodeParameterType.BOOL, True)
            # TODO: --contrastFiltering, --gridFiltering

            ui.add_separator()
            with ui.parameters_on_same_line_block():
                ui.add_parameter('rangeStart', 'Image Range Start', NodeParameterType.INT, 0)
                ui.add_parameter('rangeSize', 'Image Chunk Size', NodeParameterType.INT, 999999)


    @classmethod
    def label(cls) -> str:
        return 'AV Feature Extraction'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'alicevision', 'feature', 'extraction'

    @classmethod
    def type_name(cls) -> str:
        return 'stock_alicevision_feature_extraction'

    @classmethod
    def description(cls) -> str:
        return 'this node type does not have a description'

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        output = Path(context.param_value('output'))
        srcpath = Path(context.param_value('input'))
        if not Path(srcpath).is_absolute():
            raise ProcessingError('image directory path must be absolute and not empty')
        if not output.is_absolute():
            raise ProcessingError('output path must be absolute and not empty')

        output.mkdir(parents=True, exist_ok=True)  # TODO: move this to worker

        args = ['aliceVision_featureExtraction', '--verboseLevel', 'info']
        args += ['--describerTypes', context.param_value('describerTypes')]
        args += ['--describerPreset', context.param_value('describerPreset')]
        args += ['--describerQuality', context.param_value('describerQuality')]
        args += ['--forceCpuExtraction', context.param_value('forceCpuExtraction')]

        args += ['--rangeStart', context.param_value('rangeStart')]
        args += ['--rangeSize', context.param_value('rangeSize')]

        args += ['--input', srcpath]
        args += ['--output', output]

        job = InvocationJob([str(x) for x in args])
        res = ProcessingResult(job)
        res.set_attribute('av_features_dir', str(output))
        return res
