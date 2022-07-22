import sys
import io
import argparse
from matrix_client.client import MatrixClient


def main(argv):
    parser = argparse.ArgumentParser(description='simple matrix notifier')
    commands = parser.add_subparsers(dest='command', help='what to do')
    
    com_send = commands.add_parser('send', help='send message to a room')
    com_send.add_argument('server', help='server to use')
    com_send.add_argument('token', help='matrix session token')
    com_send.add_argument('room', help='matrix room id')
    com_send.add_argument('message', nargs='?', help='message to send')

    com_login = commands.add_parser('login', help='login and get a token to use with other commands')
    com_login.add_argument('server', help='server to use')
    com_login.add_argument('login', help='server login (user name)')
    com_login.add_argument('password', nargs='?', help='user password. will be queried if not provided')
    com_login.add_argument('deviceid', nargs='?', help='Optional. ID of the client device. The server will auto-generate a device id if this is not specified')

    args = parser.parse_args(argv)
    
    if args.command == 'login':
        password = args.password
        if password is None:
            import getpass
            password = getpass.getpass(f'enter password for {args.login}: ')
        c = MatrixClient(args.server)
        token = c.login(args.login, password, device_id=args.deviceid, sync=False)

        print(token)

    elif args.command == 'send':

        c = MatrixClient(args.server, token=args.token)

        if args.room not in c.rooms:
            c.join_room(args.room)
        r = c.rooms[args.room]
        if args.message is None:
            input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='UTF-8')
            args.message = input_stream.read()
        r.send_text(args.message)
    else:
        raise NotImplementedError(f'unknown command {args.command}')


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]) or 0)
