import sys
import sqlite3
import argparse
from .config import get_config
from . import paths


def purge_db(db_path):
    with sqlite3.connect(db_path) as con:
        con.executescript('''
UPDATE tasks SET dead=dead|2 WHERE NOT EXISTS (SELECT "group" FROM task_groups WHERE task_id==tasks."id");

PRAGMA foreign_keys=off;
DELETE FROM task_splits WHERE EXISTS (SELECT "id" FROM tasks WHERE "id"==task_splits.task_id AND dead IN (2,3));
DELETE FROM invocations WHERE EXISTS (SELECT "id" FROM tasks WHERE "id"==invocations.task_id AND dead IN (2,3));
DELETE FROM task_groups WHERE EXISTS (SELECT "group" FROM task_group_attributes WHERE "group"==task_groups."group" AND "state"==1);
DELETE FROM task_group_attributes WHERE "state"==1;

DELETE FROM tasks WHERE dead IN (2,3);
PRAGMA foreign_keys=on;
PRAGMA foreign_key_check;
VACUUM;
        ''')
        con.commit()


def info_db(db_path):
    with sqlite3.connect(db_path) as con:
        cur = con.cursor()
        cur.execute('SELECT count("id") FROM tasks')
        task_cnt = cur.fetchone()[0]
        cur.execute('SELECT count("id") FROM invocations')
        invoc_cnt = cur.fetchone()[0]
        cur.execute('SELECT count("id") FROM workers')
        worker_cnt = cur.fetchone()[0]
        cur.execute('SELECT count("group") FROM task_group_attributes')
        groups_cnt = cur.fetchone()[0]

    return {'workers count': worker_cnt,
            'tasks count': task_cnt,
            'invocations count': invoc_cnt,
            'groups count': groups_cnt}


def _main(argv):
    def confirm(prompt='continue? (y/n): '):
        confirm = None
        while confirm not in ('y', 'n'):
            confirm = input(prompt)
            confirm = confirm.lower()

        return confirm == 'y'

    parser = argparse.ArgumentParser(description='scheduler and database maintenance tools')
    parser.add_argument('-y', '--yes', help='do not ask for user confirmation for important operations. useful for scripting', action='store_true')
    t_parsergroup = parser.add_subparsers(dest='target', required=True)

    t_parser = t_parsergroup.add_parser('db', help='perform actions on scheduler database')
    c_parser = t_parsergroup.add_parser('config', help='perform actions on scheduler configuration')

    t_db_parsergroup = t_parser.add_subparsers(dest='db_command', required=True)
    t_db_info_parser = t_db_parsergroup.add_parser('info', help='print some db information')
    t_db_info_parser.add_argument('db_path', nargs='?')
    t_db_purge_parser = t_db_parsergroup.add_parser('purge', help='actually deletes all archived groups, tasks and invocations. this cannot be undone')
    t_db_purge_parser.add_argument('db_path', nargs='?')

    args = parser.parse_args(argv[1:])

    config = get_config('scheduler')
    if args.target == 'db':
        if args.db_command == 'purge':
            db_path = args.db_path if args.db_path is not None else config.get_option_noasync('scheduler.db_path', str(paths.default_main_database_location()))
            if not args.yes:
                print(f'about to purge database at {db_path}')
                print('this operation may take a lot of time, during which DB will be locked, so it is advised not to run it on a live database')
                print('this operation actually deletes all archived groups, tasks and invocations. this cannot be undone')
                if not confirm():
                    return
            print(f'purging db at {db_path}')
            purge_db(db_path)
            print(f'purge completed')
        if args.db_command == 'info':
            db_path = args.db_path if args.db_path is not None else config.get_option_noasync('scheduler.db_path', str(paths.default_main_database_location()))
            print(f'some basic information on db at {db_path}')
            info = info_db(db_path)
            for k, v in info.items():
                print(f'{k}: {v}')
            return





if __name__ == '__main__':
    sys.exit(_main(sys.argv))
