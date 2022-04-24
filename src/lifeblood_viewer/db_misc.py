sql_init_script = '''
BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS "widgets" (
    "name"    TEXT NOT NULL,
    "posx"    INTEGER,
    "posy"    INTEGER,
    "width"    INTEGER,
    "height"    INTEGER,
    "scene_x"    REAL,
    "scene_y"    REAL,
    "scene_h"    REAL,
    "scene_w"    REAL,
    PRIMARY KEY("name")
);
CREATE TABLE IF NOT EXISTS "nodes" (
    "id"    INTEGER NOT NULL,
    "posx"    REAL,
    "posy"    REAL,
    PRIMARY KEY("id")
);
COMMIT;

'''

sql_init_script_nodes = '''
BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS "nodes_{db_uid}" (
    "id"    INTEGER NOT NULL,
    "posx"    REAL,
    "posy"    REAL,
    PRIMARY KEY("id")
);
COMMIT;
'''