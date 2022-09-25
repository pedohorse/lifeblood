import asyncio
import sqlite3
import aiosqlite
import struct
import random
import json
from .environment_resolver import EnvironmentResolverArguments
from .db_misc import sql_init_script

from typing import Optional, Iterable, Any, List, Tuple, Dict

SCHEDULER_DB_FORMAT_VERSION = 1


class DataManager:
    """
    manages sqlite backend
    """

    def __init__(self, database_uri):
        self.__db_path = database_uri
        self.__db_lock_timeout = 30
        self.__db_uid = None  # unique id of the database, can be used to identify database

        self._initialize_database()

    def _initialize_database(self):
        # ensure all sql shit is initialized
        with sqlite3.connect(self.__db_path) as con:
            con.executescript(sql_init_script)
        # check if metadata exists and create or get it
        with sqlite3.connect(self.__db_path) as con:
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

    async def get_node_type_and_name_by_id(self, node_id: int) -> Tuple[str, str]:
        async with aiosqlite.connect(self.__db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('SELECT "type", "name" FROM "nodes" WHERE "id" = ?', (node_id,)) as nodecur:
                node_row = await nodecur.fetchone()
        if node_row is None:
            raise RuntimeError(f'node with given id {node_id} does not exist')
        return node_row['type'], node_row['name']

    async def get_node_object_data_by_id(self, node_id: int) -> Tuple[str, str, bytes]:
        """
        returns serialized node
        """
        async with aiosqlite.connect(self.__db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('SELECT * FROM "nodes" WHERE "id" = ?', (node_id,)) as nodecur:
                node_row = await nodecur.fetchone()
            if node_row is None:
                raise RuntimeError('node id is invalid')

            return node_row['name'], node_row['type'], node_row['node_object']

    async def set_node_object_data(self, node_id: int, *, node_name: Optional[str] = ..., node_type: Optional[str] = ..., node_data: Optional[bytes] = ...):
        """
        updates node name, type and/or serialized data in DB
        if a parameter is defaul Ellipsis (...) - it's value is NOT updated

        :param node_id:
        :param node_name:
        :param node_type:
        :param node_data:
        :return:
        """
        if node_name is None and node_type is None and node_data is None:
            return
        shit_to_set = []
        if node_name is not ...:
            shit_to_set.append(('name = ?', node_name))
        if node_type is not ...:
            shit_to_set.append(('type = ?', node_type))
        if node_data is not ...:
            shit_to_set.append(('node_object = ?', node_data))

        async with aiosqlite.connect(self.__db_path, timeout=self.__db_lock_timeout) as con:
            await con.execute(f'UPDATE "nodes" SET {", ".join(x[0] for x in shit_to_set)} WHERE "id" = ?',
                              [x[1] for x in shit_to_set])
            await con.commit()

    async def get_task_attributes(self, task_id: int) -> Tuple[Dict[str, Any], Optional[EnvironmentResolverArguments]]:
        """
        get tasks, atributes and it's enviroment resolver's attributes

        :param task_id:
        :return:
        """
        async with aiosqlite.connect(self.__db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('SELECT attributes, environment_resolver_data FROM tasks WHERE "id" = ?', (task_id,)) as cur:
                res = await cur.fetchone()
            if res is None:
                raise RuntimeError('task with specified id was not found')
            env_res_args = None
            if res['environment_resolver_data'] is not None:
                env_res_args = await EnvironmentResolverArguments.deserialize_async(res['environment_resolver_data'])
            return await asyncio.get_event_loop().run_in_executor(None, json.loads, res['attributes']), env_res_args

    async def get_task_invocation_serialized(self, task_id: int) -> Optional[bytes]:
        async with aiosqlite.connect(self.__db_path, timeout=self.__db_lock_timeout) as con:
            con.row_factory = aiosqlite.Row
            async with con.execute('SELECT work_data FROM tasks WHERE "id" = ?', (task_id,)) as cur:
                res = await cur.fetchone()
            if res is None:
                raise RuntimeError('task with specified id was not found')
            return res[0]

    async def