import asyncio


class BaseNode:
    def process_task(self, task_dict):
        raise NotImplementedError()

    def postprocess_task(self, task_dict):
        raise NotImplementedError()

    def serialize(self):
        raise NotImplementedError()
