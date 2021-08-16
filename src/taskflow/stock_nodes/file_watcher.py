import os
from math import sqrt, floor, ceil
import watchdog

from taskflow.basenode import BaseNode
from taskflow.nodethings import ProcessingResult, ProcessingError
from taskflow.uidata import NodeParameterType
from taskflow.processingcontext import ProcessingContext
from taskflow.invocationjob import InvocationJob
from taskflow.invocationjob import InvocationJob, InvocationEnvironment

from typing import Iterable


def node_class():
    return FileWatcher

watcher_code = \
'''
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
import taskflow_connection


class FileWatcher(FileSystemEventHandler):
    def on_moved(self, event):
        super(FileWatcher, self).on_moved(event)
        taskflow_connection.create_task(f'{event.src_path} moved to {event.dst_path}',
                                        src_path=event.src_path,
                                        dst_path=event.dst_path,
                                        file_event='moved')

    def on_created(self, event):
        super(FileWatcher, self).on_created(event)
        taskflow_connection.create_task(f'{event.src_path} created',
                                        src_path=event.src_path,
                                        file_event='created')

    def on_deleted(self, event):
        super(FileWatcher, self).on_deleted(event)
        taskflow_connection.create_task(f'{event.src_path} deleted',
                                        src_path=event.src_path,
                                        file_event='deleted')

    def on_modified(self, event):
        super(FileWatcher, self).on_modified(event)
        taskflow_connection.create_task(f'{event.src_path} modified',
                                        src_path=event.src_path,
                                        file_event='modified')

def main(path_to_watch, recursive):
    file_observer = Observer()
    file_observer.schedule(FileWatcher, path_to_watch, recursive)

if __name__ == '__main__':
    import sys
    sys.exit(main({dir_path}, {recursive}) or 0)

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
            with ui.parameters_on_same_line_block():
                ui.add_parameter('path', 'dir/file to watch', NodeParameterType.STRING, '')
                ui.add_parameter('recursive', 'recursive', NodeParameterType.BOOL, False)

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        inv = InvocationJob(['python', ':/watcher.py'])
        path = context.param_value('path')
        if not os.path.exists(path):
            raise ProcessingError('watch path must exist')
        if not os.path.isabs(path):
            raise ProcessingError('watch path must be absolute path')
        inv.set_extra_file('watcher.py', watcher_code.format(dir_path=path,
                                                             recursive=context.param_value('recursive')))

        return ProcessingResult(inv)

    def postprocess_task(self, context: ProcessingContext) -> ProcessingResult:
        return ProcessingResult()

