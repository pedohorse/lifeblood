from copy import copy
from lifeblood.basenode import BaseNodeWithTaskRequirements
from lifeblood.enums import NodeParameterType
from lifeblood.processingcontext import ProcessingContext
from lifeblood.invocationjob import InvocationJob, InvocationEnvironment
from lifeblood.text import filter_by_pattern

from lifeblood_stock_houdini_helpers.rop_base_node import RopBaseNode

from typing import Iterable, Optional


def node_class():
    return HipRsGenerator


class HipRsGenerator(RopBaseNode):
    @classmethod
    def description(cls) -> str:
        return 'Generates Redshift scene descriptions (.rs files) from a given houdini redshift ROP node.\n'

    @classmethod
    def label(cls) -> str:
        return 'hip rs generator'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'hip', 'houdini', 'redshift', 'rs', 'generator', 'render', 'stock'

    @classmethod
    def type_name(cls) -> str:
        return 'hip_rs_generator'

    def __init__(self, name):
        super(HipRsGenerator, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.5, 0.25, 0.125)
            ui.parameter('scene file output').set_value("`config['global_scratch_location']`/`node.name`/`task.name`_`task.id`/rs_scenes/`node.name`.$F4.rs")

            ui.parameter('worker cpu cost').set_value(0.5)
            ui.parameter('worker mem cost').set_value(1.0)
            ui.parameter('worker gpu cost').set_value(1)
            ui.parameter('worker gpu mem cost').set_value(2.0)

    def _take_parm_name(self, context) -> str:
        return 'take'

    def _parms_to_set_before_render(self, context) -> dict:
        return {
            'RS_archive_enable': True,
            'RS_renderToMPlay': False,
            'RS_archive_createDirs': True
        }

    def _scene_file_parm_name(self, context) -> str:
        return 'RS_archive_file'

    def _image_path_parm_name(self, context) -> str:
        return 'RS_outputFileNamePrefix'
