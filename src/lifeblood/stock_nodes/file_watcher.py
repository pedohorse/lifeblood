import os
from math import sqrt, floor, ceil
import re

from lifeblood.basenode import BaseNode
from lifeblood.nodethings import ProcessingResult, ProcessingError
from lifeblood.uidata import NodeParameterType
from lifeblood.processingcontext import ProcessingContext
from lifeblood.invocationjob import InvocationJob
from lifeblood.invocationjob import InvocationJob, InvocationRequirements
from lifeblood.enums import WorkerType

from typing import Iterable


def node_class():
    return FileWatcher

watcher_onmoved = \
'''
    def on_moved(self, event):
        super(FileWatcher, self).on_moved(event)
        ftype = 'dir' if event.is_directory else 'file'
        if ftype not in {ct}:
            return
        if all(not fnmatch(os.path.basename(event.src_path), x) for x in {fpats}):
            return
        lifeblood_connection.create_task(f'{{event.src_path}} moved to {{event.dest_path}}',
                                        {{'src_file': event.src_path,
                                         'file': event.dest_path,
                                         'file_event': 'moved',
                                         'file_type': ftype}})
'''

watcher_oncreated = \
'''
    def on_created(self, event):
        super(FileWatcher, self).on_created(event)
        ftype = 'dir' if event.is_directory else 'file'
        if ftype not in {ct}:
            return
        if all(not fnmatch(os.path.basename(event.src_path), x) for x in {fpats}):
            return
        lifeblood_connection.create_task(f'{{event.src_path}} created',
                                        {{'file': event.src_path,
                                         'file_event': 'created',
                                         'file_type': ftype}})
'''

watcher_ondeleted = \
'''
    def on_deleted(self, event):
        super(FileWatcher, self).on_deleted(event)
        ftype = 'dir' if event.is_directory else 'file'
        if ftype not in {ct}:
            return
        if all(not fnmatch(os.path.basename(event.src_path), x) for x in {fpats}):
            return
        lifeblood_connection.create_task(f'{{event.src_path}} deleted',
                                        {{'file': event.src_path,
                                         'file_event': 'deleted',
                                         'file_type': ftype}})
'''

watcher_onmodified = \
'''
    def on_modified(self, event):
        super(FileWatcher, self).on_modified(event)
        ftype = 'dir' if event.is_directory else 'file'
        if ftype not in {ct}:
            return
        if all(not fnmatch(os.path.basename(event.src_path), x) for x in {fpats}):
            return
        lifeblood_connection.create_task(f'{{event.src_path}} modified',
                                        {{'file': event.src_path,
                                         'file_event': 'modified',
                                         'file_type': ftype}})
'''

watcher_onclosed = \
'''
    def on_closed(self, event):
        super(FileWatcher, self).on_closed(event)
        ftype = 'dir' if event.is_directory else 'file'
        if ftype not in {ct}:
            return
        if all(not fnmatch(os.path.basename(event.src_path), x) for x in {fpats}):
            return
        lifeblood_connection.create_task(f'{{event.src_path}} modified',
                                        {{'file': event.src_path,
                                         'file_event': 'closed',
                                         'file_type': ftype}})
'''

watcher_existings = \
'''
    from pathlib import Path
    
    def _check_one_dir(dir, do_dirs, do_files, recursive):
        for file in dir.iterdir():
            if all(not fnmatch(file.name, x) for x in {fpats}):
                continue
            if file.is_dir():
                if do_dirs:
                    lifeblood_connection.create_task(f'{{file}} {event_type}',
                                                    {{'file': str(file),
                                                     'file_event': '{event_type}',
                                                     'file_type': 'dir'}})
                if recursive:
                    _check_one_dir(file, do_dirs, do_files, recursive)
            elif file.is_file() and do_files:
                lifeblood_connection.create_task(f'{{file}} {event_type}',
                                                {{'file': str(file),
                                                 'file_event': '{event_type}',
                                                 'file_type': 'file'}})

    _check_one_dir(Path(path_to_watch), {do_dirs}, {do_files}, recursive)
'''

watcher_code = \
'''
import os
from fnmatch import fnmatch
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
import lifeblood_connection


class FileWatcher(FileSystemEventHandler):
{onclosed}
{onmoved}
{oncreated}    
{ondeleted}
{onmodified}

def main(path_to_watch, recursive):
    {existings}
    file_observer = Observer()
    file_observer.schedule(FileWatcher(), path_to_watch, recursive)
    file_observer.start()
    file_observer.join()

if __name__ == '__main__':
    import sys
    sys.exit(main("{dir_path}", {recursive}) or 0)

'''


class FileWatcher(BaseNode):
    @classmethod
    def label(cls) -> str:
        return 'file watcher'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'file', 'watcher'

    @classmethod
    def type_name(cls) -> str:
        return 'file_watcher'

    def __init__(self, name):
        super(FileWatcher, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.add_output_for_spawned_tasks()
            with ui.parameters_on_same_line_block():
                ui.add_parameter('path', 'dir/file to watch', NodeParameterType.STRING, '')
                ui.add_parameter('recursive', 'recursive', NodeParameterType.BOOL, False)

            with ui.parameters_on_same_line_block():
                ui.add_parameter('doexistings', None, NodeParameterType.STRING, 'create tasks for existing items', readonly=True)
                ui.add_parameter('exist dir', 'dirs', NodeParameterType.BOOL, False)
                ui.add_parameter('exist file', 'files', NodeParameterType.BOOL, False)
                ui.add_parameter('exist pattern', 'file pattern', NodeParameterType.STRING, '*')
                ui.add_parameter('exist label', 'label for existing items', NodeParameterType.STRING, 'closed')

            with ui.parameters_on_same_line_block():
                ui.add_parameter('onclose', None, NodeParameterType.STRING, 'on close', readonly=True)
                ui.add_parameter('onclose dir', 'dirs', NodeParameterType.BOOL, False)
                ui.add_parameter('onclose file', 'files', NodeParameterType.BOOL, True)
                ui.add_parameter('onclose pattern', 'file pattern', NodeParameterType.STRING, '*')

            with ui.parameters_on_same_line_block():
                ui.add_parameter('oncreate', None, NodeParameterType.STRING, 'on create', readonly=True)
                ui.add_parameter('oncreate dir', 'dirs', NodeParameterType.BOOL, False)
                ui.add_parameter('oncreate file', 'files', NodeParameterType.BOOL, False)
                ui.add_parameter('oncreate pattern', 'file pattern', NodeParameterType.STRING, '*')
            with ui.parameters_on_same_line_block():
                ui.add_parameter('ondelete', None, NodeParameterType.STRING, 'on delete', readonly=True)
                ui.add_parameter('ondelete dir', 'dirs', NodeParameterType.BOOL, False)
                ui.add_parameter('ondelete file', 'files', NodeParameterType.BOOL, False)
                ui.add_parameter('ondelete pattern', 'file pattern', NodeParameterType.STRING, '*')
            with ui.parameters_on_same_line_block():
                ui.add_parameter('onmove', None, NodeParameterType.STRING, 'on move', readonly=True)
                ui.add_parameter('onmove dir', 'dirs', NodeParameterType.BOOL, False)
                ui.add_parameter('onmove file', 'files', NodeParameterType.BOOL, False)
                ui.add_parameter('onmove pattern', 'file pattern', NodeParameterType.STRING, '*')
            with ui.parameters_on_same_line_block():
                ui.add_parameter('onmodify', None, NodeParameterType.STRING, 'on modify', readonly=True)
                ui.add_parameter('onmodify dir', 'dirs', NodeParameterType.BOOL, False)
                ui.add_parameter('onmodify file', 'files', NodeParameterType.BOOL, False)
                ui.add_parameter('onmodify pattern', 'file pattern', NodeParameterType.STRING, '*')

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        inv = InvocationJob(['python', ':/watcher.py'], requirements=InvocationRequirements(worker_type=WorkerType.SCHEDULER_HELPER))
        path = context.param_value('path')
        if not os.path.exists(path):
            raise ProcessingError('watch path must exist')
        if not os.path.isabs(path):
            raise ProcessingError('watch path must be absolute path')
        clo_ct = []
        cre_ct = []
        del_ct = []
        mov_ct = []
        mod_ct = []
        exs_ct = []
        all = {'onclose': clo_ct, 'oncreate': cre_ct, 'ondelete': del_ct, 'onmove': mov_ct, 'onmodify': mod_ct, 'exist': exs_ct}
        for base in ('oncreate', 'ondelete', 'onmove', 'onmodify', 'onclose', 'exist'):
            for ftype in ('dir', 'file'):
                if context.param_value(f'{base} {ftype}'):
                    all[base].append(ftype)
        inv.set_extra_file('watcher.py', watcher_code.format(dir_path=path,
                                                             recursive=context.param_value('recursive'),
                                                             onclosed=watcher_onclosed.format(ct=repr(clo_ct),
                                                                                              fpats=repr(tuple(re.split('[, ]+', context.param_value(f'onclose pattern').strip())))
                                                                                              ) if len(clo_ct) > 0 else '',
                                                             onmoved=watcher_onmoved.format(ct=repr(mov_ct),
                                                                                            fpats=repr(tuple(re.split('[, ]+', context.param_value(f'onmove pattern').strip())))
                                                                                            ) if len(mov_ct) > 0 else '',
                                                             oncreated=watcher_oncreated.format(ct=repr(cre_ct),
                                                                                                fpats=repr(tuple(re.split('[, ]+', context.param_value(f'oncreate pattern').strip())))
                                                                                                ) if len(cre_ct) > 0 else '',
                                                             ondeleted=watcher_ondeleted.format(ct=repr(del_ct),
                                                                                                fpats=repr(tuple(re.split('[, ]+', context.param_value(f'ondelete pattern').strip())))
                                                                                                ) if len(del_ct) > 0 else '',
                                                             onmodified=watcher_onmodified.format(ct=repr(mod_ct),
                                                                                                  fpats=repr(tuple(re.split('[, ]+', context.param_value(f'onmodify pattern').strip())))
                                                                                                  ) if len(mod_ct) > 0 else '',
                                                             existings=watcher_existings.format(do_dirs='dir' in exs_ct,
                                                                                                do_files='file' in exs_ct,
                                                                                                event_type=context.param_value('exist label'),
                                                                                                fpats=repr(tuple(re.split('[, ]+', context.param_value('exist pattern').strip())))
                                                                                                ) if len(exs_ct) > 0 else ''
                                                             ))
        #print(inv.extra_files()['watcher.py'])
        return ProcessingResult(inv)

    def postprocess_task(self, context: ProcessingContext) -> ProcessingResult:
        return ProcessingResult()
