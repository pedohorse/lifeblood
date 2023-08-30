import sys
import os
from lifeblood.basenode import BaseNodeWithTaskRequirements, ProcessingResult, ProcessingContext, ProcessingError
from lifeblood.enums import NodeParameterType
from lifeblood.invocationjob import InvocationJob
from lifeblood.paths import config_path
from typing import Iterable
import subprocess
import time
import json
import tempfile
import shutil


description = \
'''
'''


def node_class():
    return SimpleWebServer


class SimpleWebServer(BaseNodeWithTaskRequirements):
    def __init__(self, name):
        super().__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.051, 0.741, 0.545)
            ui.add_output_for_spawned_tasks()
            with ui.parameters_on_same_line_block():
                ui.add_parameter('address', 'Listen Address', NodeParameterType.STRING, '127.0.0.1')
                ui.add_parameter('port', 'Port', NodeParameterType.INT, 8080)
            ui.add_separator()
            ui.add_parameter('expect_ready_message', 'Expect "Processing Ready" Message', NodeParameterType.BOOL, False)
            ui.add_separator()
            ui.add_parameter('page_title', 'Page Message', NodeParameterType.STRING, '<strong>File upload</strong> example')
            ui.add_parameter('base_path', 'Base Dir For Input Files', NodeParameterType.STRING, f"`config['global_scratch_location']`/{self.type_name()}/input")

    def process_task(self, context: ProcessingContext) -> ProcessingResult:
        port = context.param_value('port')
        addr = context.param_value('address')
        title = context.param_value('page_title')
        base_path = context.param_value('base_path')
        expect_reply = context.param_value('expect_ready_message')

        job = InvocationJob(['python', self.my_plugin().package_data() / "server.py",
                             '--ip', addr,
                             '--port', port,
                             '--title', title,
                             '--base_storage_path', base_path] +
                            (['--expect-reply'] if expect_reply else []))

        return ProcessingResult(job)

    @classmethod
    def type_name(cls) -> str:
        return 'stock_simple_web_server'

    @classmethod
    def label(cls) -> str:
        return 'simple web server'

    @classmethod
    def tags(cls) -> Iterable[str]:
        return 'stock', 'web', 'server'

    @classmethod
    def description(cls) -> str:
        return description
