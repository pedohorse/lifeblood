import sys
import os
import sqlite3

from PySide2.QtWidgets import QApplication
from PySide2.QtCore import QRectF, QFile

from .taskflow_viewer import TaskflowViewer
from .db_misc import sql_init_script

from .. import paths

from . import breeze_resources


def main(config_path=None):
    qapp = QApplication(sys.argv)

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

    widget = TaskflowViewer(config_path)
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


if __name__ == '__main__':
    main(config_path=os.path.join(os.getcwd(), 'node_viewer.db'))
