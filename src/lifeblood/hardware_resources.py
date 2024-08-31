import psutil
import copy
import re
import json
from .misc import get_unique_machine_id
from .logging import get_logger
from dataclasses import dataclass

from typing import Dict, List, Iterable, Optional, Tuple, Union


__mem_parse_re = re.compile(r'^\s*(\d+(?:\.\d+)?)\s*([BKMGTP]?)\s*$')


def _try_parse_value(s: Union[str, int, float], default: Optional[Union[int, float]] = None) -> Union[int, float, None]:
    if not isinstance(s, str):
        return s
    return _try_parse_mem_spec(s, default)


def _try_parse_mem_spec(s: Union[str, int], default: Optional[int] = None) -> Optional[int]:
    if not isinstance(s, str):
        return s
    match = __mem_parse_re.match(s)
    if not match:
        get_logger('worker_resources').warning(f'could not parse "{s}", using default')
        return default
    bytes_count = float(match.group(1))
    coeff = match.group(2)

    coeff_map = {'B': 1,
                 'K': 10**3,
                 'M': 10**6,
                 'G': 10**9,
                 'T': 10**12,
                 'P': 10**15}

    if coeff not in coeff_map:
        get_logger('worker_resources').warning(f'could not parse "{s}", wtf is "{coeff}"? using default')
        return default

    if coeff:
        mult = coeff_map[coeff]

        # this way we limit float op errors
        if mult > 10**6:
            mult //= 10**6
            bytes_count = int(bytes_count * 10**6)

        bytes_count = bytes_count * mult

    return int(bytes_count)


Number = Union[int, float]


@dataclass
class HardwareResource:
    value: Number


class HardwareResources:
    __resource_epsilon = 1e-5

    def __init__(self, *, hwid: Optional[int] = None, devices: Optional[Iterable[Tuple[str, str, Dict[str, Number]]]] = None, resources: Optional[Dict[str, Number]] = None):
        """

        NOTE: because of float resource rounding - it's not safe to rely on consistency of summing/subtracting a lot of resources
        """
        self.hwid = get_unique_machine_id() if hwid is None else hwid
        self.__resources: Dict[str, HardwareResource] = {}
        self.__dev_resources: List[Tuple[str, str, Dict[str, HardwareResource]]] = []
        for dev_type, dev_name, dev_res in devices or ():
            self.__dev_resources.append((dev_type, dev_name, {res_name: HardwareResource(_try_parse_value(res_val, 0)) for res_name, res_val in dev_res.items()}))
        for res_name, res_val in (resources or {}).items():
            self.__resources[res_name] = HardwareResource(res_val)

    def serialize(self) -> bytes:
        return json.dumps({
            'hwid': self.hwid,
            'res': {
                name: {'value': res.value} for name, res in self.__resources.items()
            },
            'dev': [
                [
                    dev_type,
                    dev_name,
                    {name: {'value': res.value} for name, res in dev_res.items()}
                ] for dev_type, dev_name, dev_res in self.__dev_resources
            ],
        }).encode('UTF-8')

    @classmethod
    def deserialize(cls, data_bytes: bytes) -> "HardwareResources":
        data = json.loads(data_bytes.decode('UTF-8'))
        return HardwareResources(
            hwid=data['hwid'],
            resources={name: val_dict['value'] for name, val_dict in data['res'].items()},
            devices=[
                (
                    dev_type,
                    dev_name,
                    {name: val_dict['value'] for name, val_dict in dev_res.items()},
                ) for dev_type, dev_name, dev_res in data['dev']
            ]
        )

    def devices(self) -> Iterable[Tuple[str, str, Dict[str, HardwareResource]]]:
        """
        returns tuple of "device type", "device name" and "dict of device resources"
        """
        return tuple(self.__dev_resources)

    def items(self):
        return self.__resources.items()

    def __iter__(self):
        return iter(self.__resources)

    def __len__(self):
        return len(self.__resources)

    def __getitem__(self, resource_name: str) -> HardwareResource:
        return self.__resources[resource_name]

    def __repr__(self):
        parts = []
        for res_name, res in self.__resources.items():
            parts.append(f'{res_name}: {res.value}')
        for dev_type, dev_res in self.__dev_resources.items():
            dev_parts = []
            for res_name, res in dev_res.items():
                dev_parts.append(f'{res_name}: {res.value}')
            parts.append(f'device({dev_type})[{", ".join(dev_parts)}]')

        return f'<hwid={self.hwid}, {", ".join(parts)}>'

    def __eq__(self, other):
        if not isinstance(other, HardwareResources):
            return False
        return (self.hwid == other.hwid and
                self.__resources == other.__resources and
                self.__dev_resources == other.__dev_resources)
