from logging import getLogger
from getpass import getuser
import json
import os
import re
from .lifeblood_utils_bpy import address_from_broadcast_ui
from .lifeblood_client.submitting import create_task, EnvironmentResolverArguments
import bpy

logger = getLogger('lifeblood_plugin')

bl_info = {
    'name': 'Lifeblood plugin GUI',
    'blender': (3, 4, 0),
    'category': 'Interface'
}


def expand_and_try_json_decode(value: str):
    # first expand value
    vars = dict(os.environ)
    vars['this_blend_file'] = bpy.data.filepath
    vars['frames_list'] = json.dumps(list(range(bpy.context.scene.frame_start, bpy.context.scene.frame_end + 1, bpy.context.scene.frame_step)))
    value = re.sub(r'\$(\w+|{\w+})', lambda m: vars.get(m.group(1)), value)

    # then try to decode
    try:
        return json.loads(value)
    except json.decoder.JSONDecodeError:
        return value


class LifebloodTaskAttributeItem(bpy.types.PropertyGroup):
    name: bpy.props.StringProperty(name='', default='attr name')
    val: bpy.props.StringProperty(name='', default='attr val')


def _attrib_count_get(subm, collection_name):
    return len(getattr(subm, collection_name))


def _attrib_count_set(subm, value, collection_name):
    oldval = len(getattr(subm, collection_name))

    if value > oldval:
        for _ in range(value - oldval):
            getattr(subm, collection_name).add()
    elif value < oldval:
        for _ in range(oldval - value):
            getattr(subm, collection_name).remove(len(subm.attribs) - 1)


def _set_to_stash(subm, value, attrib_name):
    if 'lifeblood_submitter_parameters' not in bpy.context.scene:
        bpy.context.scene['lifeblood_submitter_parameters'] = {}
    bpy.context.scene['lifeblood_submitter_parameters'][attrib_name] = value


def _get_from_stash(subm, attrib_name):
    return bpy.context.scene.get('lifeblood_submitter_parameters', {}).get(attrib_name, '127.0.0.1:1384')


class LifebloodSubmitOperator(bpy.types.Operator):
    bl_label = 'Lifeblood Submitter'
    bl_idname = 'wm.lifeblood_submitter'

    address: bpy.props.StringProperty(name='scheduler address',
                                      get=lambda x: _get_from_stash(x, 'address'),
                                      set=lambda x, y: _set_to_stash(x, y, 'address'))
    node_name: bpy.props.StringProperty(name='node name', default='IN BLENDER')

    task_name: bpy.props.StringProperty(name='task name', default='blender task')
    priority: bpy.props.IntProperty(name='Priority', min=0, max=100, default=50)

    attrib_count: bpy.props.IntProperty(name='Number Of Attributes', default=0, min=0,
                                        get=lambda x: _attrib_count_get(x, 'attribs'),
                                        set=lambda x, y: _attrib_count_set(x, y, 'attribs'))
    attribs: bpy.props.CollectionProperty(type=LifebloodTaskAttributeItem)

    resolver_name: bpy.props.StringProperty(name='Environment Resolver Name', default='StandardEnvironmentResolver')
    resolver_attrib_count: bpy.props.IntProperty(name='Number Of Attributes', default=0, min=0,
                                                 get=lambda x: _attrib_count_get(x, 'resolver_attribs'),
                                                 set=lambda x, y: _attrib_count_set(x, y, 'resolver_attribs'))
    resolver_attribs: bpy.props.CollectionProperty(type=LifebloodTaskAttributeItem)

    res_cpu_min: bpy.props.IntProperty(name='cpu min', default=1, min=1, soft_max=64)
    res_cpu_pref: bpy.props.IntProperty(name='cpu pref', default=8, min=1, soft_max=96)

    res_mem_min: bpy.props.IntProperty(name='mem min', default=1, min=1, soft_max=128)
    res_mem_pref: bpy.props.IntProperty(name='mem pref', default=4, min=1, soft_max=256)

    def __init__(self):
        self.attrib_count = 2
        attr = self.attribs[0]
        attr.name = 'blendfile'
        attr.val = '$this_blend_file'

        attr = self.attribs[1]
        attr.name = 'frames'
        attr.val = '$frames_list'

        self.resolver_attrib_count = 2
        attr = self.resolver_attribs[0]
        attr.name = 'user'
        attr.val = getuser()

        attr = self.resolver_attribs[1]
        attr.name = 'package.blender'
        attr.val = f'=={".".join(str(x) for x in bpy.app.version[:2])}.*'

    def draw(self, context):
        layout = self.layout
        layout.prop(self, 'address')
        layout.prop(self, 'node_name')
        layout.operator('wm.lifeblood_broadcast_listener', text='detec')

        layout.separator()

        # Attributes
        row = layout.row()
        row.prop(self, 'attrib_count')
        row.prop(self, 'attribs')
        attrbox = layout.box()
        for prop in self.attribs:
            row = attrbox.row()
            row.prop(prop, 'name')
            row.prop(prop, 'val')

        layout.separator()

        # Environment Resolver Stuff
        box = layout.box()
        box.prop(self, 'resolver_name')
        row = box.row()
        row.prop(self, 'resolver_attrib_count')
        row.prop(self, 'resolver_attribs')
        attrbox = box.box()
        for prop in self.resolver_attribs:
            row = attrbox.row()
            row.prop(prop, 'name')
            row.prop(prop, 'val')

        layout.separator()

        # Resources
        resbox = layout.box()
        row = resbox.row()
        row.prop(self, 'res_cpu_min')
        row.prop(self, 'res_cpu_pref')
        row = resbox.row()
        row.prop(self, 'res_mem_min')
        row.prop(self, 'res_mem_pref')

    def execute(self, context):
        logger.info(f'submitting to scheduler at {self.address}')

        # stash
        stash = {}
        for prop in ('address', 'node_name',
                     'resolver_name',
                     'res_cpu_min', 'res_cpu_pref', 'res_mem_min', 'res_mem_pref'):
            stash[prop] = getattr(self, prop)
        stash['attribs'] = {prop.name: prop.val for prop in self.attribs}
        stash['resolver_attribs'] = {prop.name: prop.val for prop in self.resolver_attribs}
        context.scene['lifeblood_submitter_parameters'] = stash

        # and submit
        if ':' in self.address:
            addr = self.address.split(':', 1)
            addr[1] = int(addr[1])
        else:
            addr = (self.address, 1384)

        attribs = {prop.name: expand_and_try_json_decode(prop.val) for prop in self.attribs}
        attribs['requirements'] = {'cpu': {'min': self.res_cpu_min,
                                           'pref': self.res_cpu_pref},
                                   'cmem': {'min': self.res_mem_min,
                                           'pref': self.res_mem_pref}}

        create_task(self.task_name, self.node_name, addr,
                    attribs,
                    self.resolver_name, {prop.name: expand_and_try_json_decode(prop.val) for prop in self.resolver_attribs},
                    self.priority).submit()

        return {'FINISHED'}

    def invoke(self, context, event):
        stash = context.scene.get('lifeblood_submitter_parameters')

        if stash:
            for prop in ('address', 'node_name',
                         'resolver_name',
                         'res_cpu_min', 'res_cpu_pref', 'res_mem_min', 'res_mem_pref'):
                if prop in stash:
                    setattr(self, prop, stash[prop])
            if 'attribs' in stash:
                self.attrib_count = len(stash['attribs'])
                for i, (name, val) in enumerate(stash['attribs'].items()):
                    self.attribs[i].name = name
                    self.attribs[i].val = val
            if 'resolver_attribs' in stash:
                self.resolver_attrib_count = len(stash['resolver_attribs'])
                for i, (name, val) in enumerate(stash['resolver_attribs'].items()):
                    self.resolver_attribs[i].name = name
                    self.resolver_attribs[i].val = val

        return context.window_manager.invoke_props_dialog(self)


class BroadcastListenerOperator(bpy.types.Operator):
    bl_label = 'Lifeblood Listener'
    bl_idname = 'wm.lifeblood_broadcast_listener'

    address: bpy.props.StringProperty(name='address', default='0.0.0.0')
    port: bpy.props.IntProperty(name='port', default=34305)
    timeout: bpy.props.IntProperty(name='listen to broadcast timeout', default=10, min=1, max=30)

    def execute(self, context):
        logger.debug('starting listening to the broadcast')
        if 'lifeblood_submitter_parameters' not in context.scene:
            context.scene['lifeblood_submitter_parameters'] = {}
        address, port = address_from_broadcast_ui((self.address, self.port), timeout=self.timeout)
        if address is not None:
            context.scene['lifeblood_submitter_parameters']['address'] = f'{address}:{port}'
            logger.debug(f'broadcast received: {address}:{port}')
            self.report({'INFO'}, 'caught a broadcast!')
        else:
            logger.debug('broadcast timeout')
            self.report({'WARNING'}, 'broadcast listening timed out!')

        return {'FINISHED'}

    def invoke(self, context, event):
        return context.window_manager.invoke_props_dialog(self)

    def draw(self, context):
        layout = self.layout
        layout.label(text='listen to broadcast')
        row = layout.row()
        row.prop(self, 'address')
        row.prop(self, 'port')
        layout.prop(self, 'timeout')


def lifeblood_main_menu_items(self, context):
    self.layout.operator('wm.lifeblood_submitter')


def register():
    logger.info('registering lifeblood plugin...')
    bpy.utils.register_class(LifebloodTaskAttributeItem)
    bpy.utils.register_class(BroadcastListenerOperator)
    bpy.utils.register_class(LifebloodSubmitOperator)
    bpy.types.TOPBAR_MT_render.append(lifeblood_main_menu_items)


def unregister():
    logger.info('unregistering lifeblood plugin...')
    bpy.types.TOPBAR_MT_render.remove(lifeblood_main_menu_items)
