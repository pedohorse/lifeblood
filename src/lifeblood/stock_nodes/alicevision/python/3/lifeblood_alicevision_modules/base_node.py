from lifeblood.basenode import BaseNodeWithTaskRequirements
import shutil
from pathlib import Path


class AlicevisionBaseNode(BaseNodeWithTaskRequirements):
    def __init__(self, name):
        super(AlicevisionBaseNode, self).__init__(name)
        ui = self.get_ui()
        with ui.initializing_interface_lock():
            ui.color_scheme().set_main_color(0.15, 0.15, 0.6)
