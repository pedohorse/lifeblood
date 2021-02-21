import asyncio
import pickle


class InvocationJob:
    """
    serializable data about launching something
    """
    def __init__(self, args: list, env: dict, invocation_id=None):
        self.__args = args
        self.__env = env
        self.__invocation_id = invocation_id
        # TODO: add here also all kind of resource requirements information

    def args(self):
        return self.__args

    def env(self):
        return self.__env

    def invocation_id(self):
        return self.__invocation_id

    async def serialize(self) -> bytes:
        return await asyncio.get_event_loop().run_in_executor(None, pickle.dumps, self)

    def set_invocation_id(self, invocation_id):
        self.__invocation_id = invocation_id

    def __repr__(self):
        return 'InvocationJob: %d, %s %s' % (self.__invocation_id, repr(self.__args), repr(self.__env))

    @classmethod
    def deserialize(cls, data: bytes) -> "InvocationJob":
        return pickle.loads(data)
