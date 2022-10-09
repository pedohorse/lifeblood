import sys
import os
import sqlite3

from PySide2.QtWidgets import QApplication
from PySide2.QtCore import QRectF, QFile, Qt

from .lifeblood_viewer import LifebloodViewer
from .db_misc import sql_init_script

from lifeblood import paths
from lifeblood.config import create_default_user_config_file

from . import breeze_resources


default_config = '''
[viewer]
listen_to_broadcast = true

[imgui]
# ini_file = "path/to/some/special/imgui.ini"
'''


def main(argv):  # TODO: parse cmdline (argv)
    # config_path = os.path.join(os.getcwd(), 'node_viewer.db')
    create_default_user_config_file('viewer', default_config)
    start_viewer()  # config_path=config_path)


def start_viewer(config_path=None):
    qapp = QApplication(sys.argv)

    qapp.setAttribute(Qt.AA_CompressHighFrequencyEvents, False)  # fixes the bug of accumulating wheel events

    # set stylesheet
    ssfile = QFile(":/dark.qss")
    ssfile.open(QFile.ReadOnly | QFile.Text)
    try:
        stylesheet = str(ssfile.readAll(), 'UTF-8')
    finally:
        ssfile.close()
    qapp.setStyleSheet(stylesheet)
    #

    if config_path is None:
        config_path = paths.config_path('node_viewer.db', 'viewer')
    if not os.path.exists(config_path):
        with open(config_path, 'w') as f:
            pass

    hgt, wgt = None, None
    posx, posy = None, None
    scene_rect = None
    with sqlite3.connect(config_path) as con:
        con.executescript(sql_init_script)

        con.row_factory = sqlite3.Row
        cur = con.execute('SELECT * FROM widgets WHERE "name" = ?', ('main',))
        row = cur.fetchone()
        if row is not None:
            hgt = row['height']
            wgt = row['width']
            posx = row['posx']
            posy = row['posy']
            if row['scene_x'] is not None:
                scene_rect = QRectF(row['scene_x'], row['scene_y'], 1, 1)

    widget = LifebloodViewer(config_path)
    if hgt is not None:
        widget.resize(wgt, hgt)
    if posx is not None:
        widget.move(posx, posy)
    if scene_rect is not None:
        widget.setSceneRect(scene_rect)
    widget.show()

    qapp.exec_()
    with sqlite3.connect(config_path) as con:
        scene_rect = widget.sceneRect()
        con.execute('INSERT OR REPLACE INTO widgets ("name", "width", "height", "posx", "posy", '
                    '"scene_x", "scene_y", "scene_w", "scene_h") '
                    'VALUES (?, ?, ?, ?, ?, '
                    '?, ?, ?, ?)',
                    ('main', *widget.size().toTuple(), *widget.pos().toTuple(),
                     *scene_rect.topLeft().toTuple(), *scene_rect.size().toTuple()))
        con.commit()


def console_entry_point():
    sys.exit(main(sys.argv[1:]))


if __name__ == '__main__':
    console_entry_point()
