from lifeblood.basenode import ProcessingResult, ProcessingContext, ProcessingError
from lifeblood.invocationjob import InvocationJob
from lifeblood.enums import NodeParameterType
from lifeblood_alicevision_modules.base_node import AlicevisionBaseNode
from pathlib import Path
from typing import Iterable


def node_class():
    return AlicevisionImageMatching


class AlicevisionImageMatching(AlicevisionBaseNode):
    def __init__(self, name):
        super(AlicevisionImageMatching, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('input', 'Input Images Directory', NodeParameterType.STRING, "`task['av_camera_sfm']`")
            ui.add_parameter('featuresFolders', 'Features Dir', NodeParameterType.STRING, "`task['av_features_dir']`")  # TODO: this seem to be an array of dirs, so add support
            ui.add_parameter('output', 'Input Images Directory', NodeParameterType.STRING, "`config['global_scratch_location']`/alicevision/`task['uuid']`/ImageMatching/imageMatches.txt")
            ui.add_separator()
            ui.add_parameter('method', 'Method', NodeParameterType.STRING, 'VocabularyTree')\
                .add_menu((('Vocabulary Tree', 'VocabularyTree'),
                           ('Sequential', 'Sequential'),
                           ('Sequential + Vocabulary Tree', 'SequentialAndVocabularyTree'),
                           ('Exhaustive', 'Exhaustive'),
                           ('Frustum', 'Frustum'),
                           ('Frustum or Vocabulary Tree', 'FrustumOrVocabularyTree')))
            ui.add_parameter('tree', 'Vocabulary Tree', NodeParameterType.STRING, '../../aliceVision/share/aliceVision/vlfeat_K80L3.SIFT.tree')
            ui.add_parameter('minNbImages', 'minNbImages', NodeParameterType.INT, 200)
            ui.add_parameter('maxDescriptors', 'maxDescriptors', NodeParameterType.INT, 500)
            ui.add_parameter('nbMatches', 'nbMatches', NodeParameterType.INT, 50)
            ui.add_parameter('weights', 'weights', NodeParameterType.STRING, '')

    @classmethod
    def label(cls) -> str:
        return 'AV Image Matching'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'alicevision', 'image', 'matching'

    @classmethod
    def type_name(cls) -> str:
        return 'stock_alicevision_image_matching'

    @classmethod
    def description(cls) -> str:
        return 'this node type does not have a description'

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        tree_path = Path(context.param_value('tree'))
        output = Path(context.param_value('output').strip())
        if not output.is_absolute():
            raise ProcessingError('output path must be absolute and not empty')

        output.parent.mkdir(parents=True, exist_ok=True)  # TODO: move this to worker

        args = ['aliceVision_imageMatching', '--verboseLevel', 'info']
        args += ['--input', context.param_value('input')]
        args += ['--featuresFolder', context.param_value('featuresFolders')]
        args += ['--output', output]
        args += ['--method', context.param_value('method')]
        args += ['--tree', tree_path]
        args += ['--minNbImages', context.param_value('minNbImages')]
        args += ['--maxDescriptors', context.param_value('maxDescriptors')]
        args += ['--nbMatches', context.param_value('nbMatches')]
        args += ['--weights', context.param_value('weights')]

        job = InvocationJob([str(x) for x in args])
        res = ProcessingResult(job)
        res.set_attribute('av_image_matches', str(output))
        return res
