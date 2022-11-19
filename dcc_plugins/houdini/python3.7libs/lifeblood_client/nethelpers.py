import struct
import socket


def recv_exactly(sock, numbytes):  # type: (socket.socket, int) -> bytes
    patches = []
    got_bytes = 0
    while got_bytes != numbytes:
        patches.append(sock.recv(numbytes - got_bytes))
        got_bytes += len(patches[-1])
        if len(patches[-1]) == 0:
            raise ConnectionResetError()

    if len(patches) == 1:
        return patches[0]
    elif len(patches) == 0:
        return b''
    return b''.join(patches)


def send_string(sock, text):  # type: (socket.socket, str) -> None
    bts = text.encode('UTF-8')
    sock.sendall(struct.pack('>Q', len(bts)))
    sock.sendall(bts)


def recv_string(sock):  # type: (socket.socket) -> str
    btlen = struct.unpack('>Q', recv_exactly(sock, 8))[0]
    return recv_exactly(sock, btlen).decode('UTF-8')
