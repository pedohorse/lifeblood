import json
import socket
import struct
import time
import threading
from typing import Tuple


def address_from_broadcast(broadcast_address: Tuple[str, int], awaited_id='lifeblood_scheduler', timeout=10):
    magic = b'=\xe2\x88\x88\xe2\xad\x95\xe2\x88\x8b='
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    s.settimeout(timeout)
    s.bind(broadcast_address)

    start_time = time.time()
    for _ in range(999):  # unreasonably big but finite number
        s.settimeout(max(0.01, timeout - (time.time() - start_time)))
        try:
            data = s.recv(1024)
        except socket.timeout:
            return None, None

        if not data.startswith(magic):
            continue
        data = data[len(magic):]
        idlen, infolen = struct.unpack('>II', data[:8])
        id = data[8:8 + idlen].decode('UTF-8')
        if id != awaited_id:
            continue
        info = json.loads(data[8 + idlen:8 + idlen + infolen].decode('UTF-8'))
        ip, port = info.get('worker', '127.0.0.1:1384').split(':')
        return ip, int(port)
