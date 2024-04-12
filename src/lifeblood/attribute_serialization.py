import asyncio
import json

from .common_serialization import AttribSerializer, AttribDeserializer


async def serialize_attributes(attributes: dict) -> str:
    return await asyncio.get_event_loop().run_in_executor(None, serialize_attributes_core, attributes)


async def deserialize_attributes(attributes_serialized: str) -> dict:
    return await asyncio.get_event_loop().run_in_executor(None, deserialize_attributes_core, attributes_serialized)


def serialize_attributes_core(attributes: dict) -> str:
    return json.dumps(attributes, cls=AttribSerializer)


def deserialize_attributes_core(attributes_serialized: str) -> dict:
    return json.loads(attributes_serialized, cls=AttribDeserializer)
