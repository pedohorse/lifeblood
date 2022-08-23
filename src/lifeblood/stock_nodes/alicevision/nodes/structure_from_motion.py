from lifeblood.basenode import ProcessingResult, ProcessingContext, ProcessingError
from lifeblood.invocationjob import InvocationJob
from lifeblood.enums import NodeParameterType
from lifeblood_alicevision_modules.base_node import AlicevisionBaseNode
from pathlib import Path
from typing import Iterable


def node_class():
    return AlicevisionStructureFromMotion


class AlicevisionStructureFromMotion(AlicevisionBaseNode):
    def __init__(self, name):
        super(AlicevisionStructureFromMotion, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('input', 'Input Images Directory', NodeParameterType.STRING, "`task['av_camera_sfm']`")
            ui.add_parameter('featuresFolders', 'Features Dir', NodeParameterType.STRING, "`task['av_features_dir']`")  # TODO: this seem to be an array of dirs, so add support
            ui.add_parameter('matchesFolders', 'Matches Dir', NodeParameterType.STRING, "`task['av_feature_matches']`")  # TODO: this seem to be an array of dirs, so add support
            ui.add_parameter('output', 'Output Structure From Motion', NodeParameterType.STRING, "`config['global_scratch_location']`/alicevision/`task['uuid']`/StructureFromMotion/sfm.abc")
            ui.add_parameter('outputViewsAndPoses', 'Output Views And Poses', NodeParameterType.STRING, "`config['global_scratch_location']`/alicevision/`task['uuid']`/StructureFromMotion/cameras.sfm")
            ui.add_parameter('extraInfoFolder', 'Output Folder', NodeParameterType.STRING, "`config['global_scratch_location']`/alicevision/`task['uuid']`/StructureFromMotion")
            ui.add_separator()
            ui.add_parameter('describerTypes', 'Describer Types', NodeParameterType.STRING, 'sift')  # TODO: make a set of checkboxes
            ui.add_parameter('lockScenePreviouslyReconstructed', 'Lock Scene Previously Reconstructed', NodeParameterType.BOOL, False)
            ui.add_parameter('useLocalBA', 'Local Bundle Adjustment', NodeParameterType.BOOL, True)
            ui.add_parameter('maxNumberOfMatches', 'Max Number Of Matches', NodeParameterType.INT, 0)
            ui.add_parameter('minNumberOfMatches', 'Min Number Of Matches', NodeParameterType.INT, 0)
            ui.add_parameter('minInputTrackLength', 'Min Input Track Length', NodeParameterType.INT, 2).set_value_limits(2)
            ui.add_parameter('lockAllIntrinsics', 'Force Lock of All Intrinsic Camera Parameters', NodeParameterType.BOOL, False)
            ui.add_parameter('filterTrackForks', 'Filter Track Forks', NodeParameterType.BOOL, False)
            with ui.parameters_on_same_line_block():
                ui.add_parameter('initialPairA', 'Initial Pair', NodeParameterType.STRING, '')
                ui.add_parameter('initialPairB', None, NodeParameterType.STRING, '')


    @classmethod
    def label(cls) -> str:
        return 'AV Structure From Motion'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'alicevision', 'structure', 'motion', 'sfm'

    @classmethod
    def type_name(cls) -> str:
        return 'stock_alicevision_structure_from_motion'

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
        output_folder = Path(context.param_value('extraInfoFolder'))
        if not output_folder.is_absolute():
            raise ProcessingError('output path must be absolute and not empty')

        output.parent.mkdir(parents=True, exist_ok=True)  # TODO: move this to worker
        output_vap.parent.mkdir(parents=True, exist_ok=True)  # TODO: move this to worker
        output_folder.mkdir(parents=True, exist_ok=True)  # TODO: move this to worker

        args = ['aliceVision_incrementalSfM', '--verboseLevel', 'info']
        for param_name in ('input', 'featuresFolders', 'matchesFolders',
                           'describerTypes', 'lockScenePreviouslyReconstructed', 'useLocalBA',
                           'maxNumberOfMatches', 'minNumberOfMatches', 'minInputTrackLength',
                           'lockAllIntrinsics', 'filterTrackForks', 'initialPairA', 'initialPairB'):
            args += [f'--{param_name}', context.param_value(param_name)]
        args += ['--output', output]
        args += ['--outputViewsAndPoses', output_vap]
        args += ['--extraInfoFolder', output_folder]

        job = InvocationJob([str(x) for x in args])
        res = ProcessingResult(job)
        res.set_attribute('av_structure_from_motion', str(output))
        res.set_attribute('av_viewes_and_poses', str(output_vap))
        res.set_attribute('av_extra_info_folder', str(output_folder))
        return res
