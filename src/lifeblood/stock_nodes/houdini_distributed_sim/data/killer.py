import sys
import lifeblood_connection


def main(invocation_iid: int, addressee: str, timeout: float = 90):
    lifeblood_connection.message_to_invocation_send(invocation_iid, addressee, b'stop', timeout)


if __name__ == '__main__':
    main(int(sys.argv[1]), sys.argv[2], float(sys.argv[3]))
