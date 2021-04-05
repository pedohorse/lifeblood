import sys
import os
import sqlite3

from PySide2.QtWidgets import QApplication
from PySide2.QtCore import QRectF

from .taskflow_viewer import TaskflowViewer
from .db_misc import sql_init_script


def main():
    qapp = QApplication(sys.argv)

    db_path = os.path.join(os.getcwd(), 'node_viewer.db')

    hgt, wgt = None, None
    posx, posy = None, None
    scene_rect = None
    with sqlite3.connect(db_path) as con:
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
                scene_rect = QRectF(row['scene_x'], row['scene_y'], row['scene_w'], row['scene_h'])

    widget = TaskflowViewer(db_path)
    if hgt is not None:
        widget.resize(wgt, hgt)
    if posx is not None:
        widget.move(posx, posy)
    if scene_rect is not None:
        widget.setSceneRect(scene_rect)
    widget.show()

    qapp.exec_()
    with sqlite3.connect(db_path) as con:
        scene_rect = widget.sceneRect()
        con.execute('INSERT OR REPLACE INTO widgets ("name", "width", "height", "posx", "posy", '
                    '"scene_x", "scene_y", "scene_w", "scene_h") '
                    'VALUES (?, ?, ?, ?, ?, '
                    '?, ?, ?, ?)',
                    ('main', *widget.size().toTuple(), *widget.pos().toTuple(),
                     *scene_rect.topLeft().toTuple(), *scene_rect.size().toTuple()))
        con.commit()


if __name__ == '__main__':
    main()
