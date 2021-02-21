import asyncio
import pickle


async def create_uidata(raw_nodes, raw_connections, raw_tasks):
    return await asyncio.get_event_loop().run_in_executor(None, UiData, raw_nodes, raw_connections, raw_tasks)


class UiData:
    def __init__(self, raw_nodes, raw_connections, raw_tasks):
        self.__nodes = {x['id']: dict(x) for x in raw_nodes}
        self.__conns = {x['id']: dict(x) for x in raw_connections}
        self.__tasks = {x['id']: dict(x) for x in raw_tasks}
        # self.__conns = {}
        # for conn in raw_connections:
        #     id_out = conn['node_id_out']
        #     id_in = conn['node_id_in']
        #     if id_out not in self.__conns:
        #         self.__conns[id_out] = {}
        #     if id_in not in self.__conns[id_out]:
        #         self.__conns[id_out][id_in] = []
        #     self.__conns[id_out][id_in].append(dict(conn))

    def nodes(self):
        return self.__nodes

    def connections(self):
        return self.__conns

    def tasks(self):
        return self.__tasks

    async def serialize(self) -> bytes:
        return await asyncio.get_event_loop().run_in_executor(None, pickle.dumps, self)

    def __repr__(self):
        return f'{self.__nodes} :::: {self.__conns}'

    @classmethod
    def deserialize(cls, data: bytes) -> "UiData":
        return pickle.loads(data)
