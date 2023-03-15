import aiosqlite
import sqlite3
import random
import struct
from ..db_misc import sql_init_script
from ..logging import get_logger
from ..scheduler_event_log import SchedulerEventLog
from ..shared_lazy_sqlite_connection import SharedLazyAiosqliteConnection
from .. import aiosqlite_overlay

SCHEDULER_DB_FORMAT_VERSION = 1


class DataAccess:
    def __init__(self, db_path, db_connection_timeout):
        self.__logger = get_logger('scheduler.data_access')
        self.db_path: str = db_path
        self.db_timeout: int = db_connection_timeout

        self.mem_cache_workers_resources: dict = {}
        self.mem_cache_workers_state: dict = {}
        self.mem_cache_invocations: dict = {}
        #
        # ensure database is initialized
        with sqlite3.connect(db_path) as con:
            con.executescript(sql_init_script)
        with sqlite3.connect(db_path) as con:
            con.row_factory = sqlite3.Row
            cur = con.execute('SELECT * FROM lifeblood_metadata')
            metadata = cur.fetchone()  # there should be exactly one single row.
            cur.close()
            if metadata is None:  # if there's no - the DB has not been initialized yet
                # we need 64bit signed id to save into db
                db_uid = random.getrandbits(64)  # this is actual db_uid
                db_uid_signed = struct.unpack('>q', struct.pack('>Q', db_uid))[0]  # this one goes to db
                con.execute('INSERT INTO lifeblood_metadata ("version", "component", "unique_db_id")'
                            'VALUES (?, ?, ?)', (SCHEDULER_DB_FORMAT_VERSION, 'scheduler', db_uid_signed))
                con.commit()
                # reget metadata
                cur = con.execute('SELECT * FROM lifeblood_metadata')
                metadata = cur.fetchone()  # there should be exactly one single row.
                cur.close()
            self.__db_uid = struct.unpack('>Q', struct.pack('>q', metadata['unique_db_id']))[0]  # reinterpret signed as unsigned

    @property
    def db_uid(self):
        return self.__db_uid

    def data_connection(self) -> aiosqlite_overlay.ConnectionWithCallbacks:
        return aiosqlite_overlay.connect(self.db_path, timeout=self.db_timeout, pragmas_after_connect=('synchronous=NORMAL',))

    def lazy_data_transaction(self, key_name: str):
        return SharedLazyAiosqliteConnection(None, self.db_path, key_name, timeout=self.db_timeout)

    async def write_back_cache(self):
        self.__logger.info('pinger syncing temporary tables back...')
        async with self.data_connection() as con:
            for wid, cached_row in self.mem_cache_workers_state.items():
                await con.execute('UPDATE workers SET '
                                  'last_seen=?, '
                                  'last_checked=?, '
                                  'ping_state=? '
                                  'WHERE "id"=?',
                                  (cached_row['last_seen'], cached_row['last_checked'], cached_row['ping_state'], wid))
            await con.commit()
