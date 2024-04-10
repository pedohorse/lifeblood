# NOTE: this file is a simplified standalone version of lifeblood.attribute_serialization
# And therefore this file should be kept up to date with the original
#
# note as well: this is still kept py2-3 compatible
import json

from .common_serialization import AttribSerializer, AttribDeserializer


def serialize_attributes_core(attributes: dict) -> str:
    return json.dumps(attributes, cls=AttribSerializer)  # TODO: allow SOME custom object serialization


def deserialize_attributes_core(attributes_serialized: str) -> dict:
    return json.loads(attributes_serialized, cls=AttribDeserializer)
