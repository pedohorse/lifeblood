import platform


if platform.system().lower() == 'windows':
    import socket
    # now before you go all crazy about the line below - we ONLY use SO_REUSEPORT in broadcasting
    # otherwise it shouldn't break anything....
    # but yeah....
    socket.SO_REUSEPORT = socket.SO_REUSEADDR
