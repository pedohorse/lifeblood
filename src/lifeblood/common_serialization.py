import json


class AttribSerializer(json.JSONEncoder):
    def _reform(self, obj):
        if type(obj) is set:
            return {
                '__special_object_type__': 'set',
                'items': self._reform(list(obj))
            }
        elif type(obj) is tuple:
            return {
                '__special_object_type__': 'tuple',
                'items': self._reform(list(obj))
            }
        elif type(obj) is dict:  # int keys case
            if any(isinstance(x, (int, float, tuple)) for x in obj.keys()):
                return {
                    '__special_object_type__': 'kvp',
                    'items': self._reform([[k, v] for k, v in obj.items()])
                }
            return {k: self._reform(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._reform(x) for x in obj]
        elif isinstance(obj, (int, float, str, bool)) or obj is None:
            return obj
        raise NotImplementedError(f'serialization not implemented for type "{type(obj)}"')

    def encode(self, o):
        return super().encode(self._reform(o))

    def default(self, obj):
        return super().default(obj)


class AttribDeserializer(json.JSONDecoder):
    def _dedata(self, obj):
        special_type = obj.get('__special_object_type__')
        if special_type == 'set':
            return set(obj.get('items'))
        elif special_type == 'tuple':
            return tuple(obj.get('items'))
        elif special_type == 'kvp':
            return {k: v for k, v in obj.get('items')}
        return obj

    def __init__(self):
        super().__init__(object_hook=self._dedata)
