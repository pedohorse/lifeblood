import sys
import io
import argparse
from matrix_client.client import MatrixClient


def main(argv):
    parser = argparse.ArgumentParser(description='simple matrix notifier')
    parser.add_argument('server', help='server to use')
    parser.add_argument('token', help='matrix session token')
    parser.add_argument('room', help='matrix room id')
    parser.add_argument('message', nargs='?', help='message to send')
    args = parser.parse_args(argv)
    c = MatrixClient(args.server, token=args.token)

    if args.room not in c.rooms:
        c.join_room(args.room)
    r = c.rooms[args.room]
    if args.message is None:
        input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='UTF-8')
        args.message = input_stream.read()
    r.send_text(args.message)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]) or 0)
