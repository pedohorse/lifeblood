from lifeblood.basenode import ProcessingResult, ProcessingContext, ProcessingError
from lifeblood.invocationjob import InvocationJob
from lifeblood.enums import NodeParameterType
from lifeblood_alicevision_modules.base_node import AlicevisionBaseNode
from pathlib import Path
from typing import Iterable


def node_class():
    return AlicevisionPrepareDenseScene


class AlicevisionPrepareDenseScene(AlicevisionBaseNode):
    def __init__(self, name):
        super(AlicevisionPrepareDenseScene, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_parameter('input', 'Input Images Directory', NodeParameterType.STRING, "`task['av_structure_from_motion']`")
            ui.add_parameter('', 'Images Folders', NodeParameterType.STRING, "")  # TODO: make a set of checkboxes
            ui.add_parameter('output', 'Output Image Folder', NodeParameterType.STRING, "`config['global_scratch_location']`/alicevision/`task['uuid']`/PrepareDenseScene")
            ui.add_parameter('output', 'Output Undistorted Images', NodeParameterType.STRING, "`config['global_scratch_location']`/alicevision/`task['uuid']`/StructureFromMotion/sfm.abc")