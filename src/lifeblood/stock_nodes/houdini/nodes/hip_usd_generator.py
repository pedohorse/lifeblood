from copy import copy
from lifeblood.basenode import BaseNodeWithTaskRequirements
from lifeblood.enums import NodeParameterType
from lifeblood.processingcontext import ProcessingContext
from lifeblood.invocationjob import InvocationJob, InvocationEnvironment
from lifeblood.text import filter_by_pattern

from lifeblood_stock_houdini_helpers.rop_base_node import RopBaseNode

from typing import Iterable, Optional


def node_class():
    return HipUsdGenerator


class HipUsdGenerator(RopBaseNode):
    @classmethod
    def label(cls) -> str:
        return 'usd generator'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'hip', 'houdini', 'usd', 'lop', 'solaris', 'generator', 'render', 'stock'

    @classmethod
    def type_name(cls) -> str:
        return 'hip_usd_generator'

    def __init__(self, name):
        super(HipUsdGenerator, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.5, 0.25, 0.125)

            ui.parameter('scene file output').set_value("`config['global_scratch_location']`/`node.name`/`task.name`_`task.id`/usd/`node.name`_$F4")

            skip_param = ui.parameter('skip if exists')
            skip_param.set_value(False)
            skip_param.set_locked(True)
            skip_param.set_hidden(True)

            ui.add_separator()

            with ui.parameters_on_same_line_block():
                use_custom = ui.add_parameter('use custom usd attr for img', 'Custom USD prim/attr for output image', NodeParameterType.BOOL, False)
                ui.add_parameter('custom usd attr for img', None, NodeParameterType.STRING, '/Render/Products/renderproduct/productName')

    # that below - for mantra
    #
    # def _take_parm_name(self, context) -> str:
    #     return 'take'
    #
    # def _parms_to_set_before_render(self, context) -> dict:
    #     return {'soho_outputmode': 1,
    #             'vm_inlinestorage': int(context.param_value('ifd force inline'))}
    #
    # def _scene_file_parm_name(self, context) -> str:
    #     return 'soho_diskfile'
    #
    # def _image_path_parm_name(self, context) -> str:
    #     return 'vm_picture'

    def _take_parm_name(self, context) -> str:
        return 'take'

    def _parms_to_set_before_render(self, context) -> dict:
        return {'mkpath': True,
                'runcommand': False,
                'husk_mplay': False
                }

    def _scene_file_getting_code(self, context) -> Optional[str]:
        return '\n'.join([f'base = node.evalParm("savetodirectory_directory")',
                          f'filename_parm = node.parm("lopoutput")',
                          f'if filename_parm is not None:',
                          f'    filename = filename_parm.eval()',
                          f'else:',
                          f'    filename = "render.usd"',
                          f'if os.path.splitext(filename) == ".usd" and hou.licenseCategory() in (hou.licenseCategoryType.Apprentice, hou.licenseCategoryType.ApprenticeHD):',
                          f'    filename += "nc"',
                          f'return os.path.join(base, filename)'])

    def _scene_file_parm_name(self, context) -> str:
        return 'savetodirectory_directory'

    def _image_getting_code(self, context: ProcessingContext) -> Optional[str]:
        usd_attr_path = context.param_value('custom usd attr for img') if context.param_value('use custom usd attr for img') else '/Render/Products/renderproduct/productName'
        prim_path, attr_name = usd_attr_path.rsplit('/', 1)
        return '\n'.join([f'override = node.parm("outputimage").evalAsString().strip()',
                          f'if override:',
                          f'    return override',
                          f'prim = node.input(0).stage().GetPrimAtPath({repr(prim_path)})',
                          f'attr = prim.GetAttribute({repr(attr_name)})',
                          f'return attr.Get(frame)'])

    def _node_specialization_code(self, context):
        return '\n'.join([f'if node.type().name() == "karma":',
                          f'    if node.type().category() == hou.nodeTypeCategories()["Driver"]:',
                          f'        return node.node("lopnet/karma/rop_usdrender")',
                          f'    elif node.type().category() == hou.nodeTypeCategories()["Lop"]:',
                          f'        return node.node("rop_usdrender")',
                          f'return node'])

    def _image_path_parm_name(self, context) -> str:
        return ''  # value ignored cuz _image_getting_code is returning non None
