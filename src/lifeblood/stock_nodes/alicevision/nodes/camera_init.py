from lifeblood.basenode import ProcessingResult, ProcessingContext, ProcessingError
from lifeblood.invocationjob import InvocationJob
from lifeblood.enums import NodeParameterType
from lifeblood_alicevision_modules.base_node import AlicevisionBaseNode
from pathlib import Path
from typing import Iterable


def node_class():
    return AlicevisionCameraInit


class AlicevisionCameraInit(AlicevisionBaseNode):
    def __init__(self, name):
        super(AlicevisionCameraInit, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('inputFolder', 'Input Images Directory', NodeParameterType.STRING, '')
            ui.add_parameter('output', 'Output Camera', NodeParameterType.STRING, "`config['global_scratch_location']`/alicevision/`task['uuid']`/CameraInit/cameraInit.sfm")
            ui.add_separator()
            ui.add_parameter('useInternalWhiteBalance', 'Use Internal White Balance', NodeParameterType.BOOL, True)
            ui.add_parameter('defaultFieldOfView', 'Default Field of View', NodeParameterType.FLOAT, 45)
            ui.add_parameter('sensorDatabase', 'Sensor Database', NodeParameterType.STRING, '../../aliceVision/share/aliceVision/cameraSensors.db')

    @classmethod
    def label(cls) -> str:
        return 'AV Camera Init'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'alicevision', 'camera', 'init'

    @classmethod
    def type_name(cls) -> str:
        return 'stock_alicevision_camera_init'

    @classmethod
    def description(cls) -> str:
        return 'this node type does not have a description'

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        output = Path(context.param_value('output').strip())
        srcpath = context.param_value('inputFolder').strip()
        if srcpath == '' or not Path(srcpath).is_absolute():
            raise ProcessingError('image directory path must be absolute and not empty')
        if not output.is_absolute():
            raise ProcessingError('output path must be absolute and not empty')

        sensor_db_path = context.param_value('sensorDatabase').strip()

        output.parent.mkdir(parents=True, exist_ok=True)  # TODO: move this to worker

        args = ['aliceVision_cameraInit', '--verboseLevel', 'info']
        args += ['--defaultFieldOfView', context.param_value('defaultFieldOfView')]
        args += ['--allowSingleView', 1]
        args += ['--sensorDatabase', sensor_db_path]
        args += ['--useInternalWhiteBalance', context.param_value('useInternalWhiteBalance')]
        args += ['--imageFolder', srcpath]
        args += ['--output', output]

        job = InvocationJob([str(x) for x in args])
        res = ProcessingResult(job)
        res.set_attribute('av_camera_sfm', str(output))
        return res
