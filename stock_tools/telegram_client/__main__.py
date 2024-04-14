#!/usr/bin/env python
import os
import sys
import argparse
from telebot import TelegramClient


def main():
    parser = argparse.ArgumentParser(description='micro telegram client')
    parser.add_argument('--message', default='')
    parser.add_argument('--message-stdin', action='store_true')
    parser.add_argument('--attach', default=None)
    parser.add_argument('--status', action='store_true')
    parser.add_argument('chat_id')

    args = parser.parse_args()

    secret = os.environ['TELEGRAM_CLIENT_BOTID']
    if not secret:
        raise RuntimeError('telegram bot_id must be passed as TELEGRAM_CLIENT_BOTID env var')

    client = TelegramClient(secret)

    if args.status:
        print(client.get_me())
        return

    if args.message_stdin:
        message = sys.stdin.read()
    else:
        message = args.message

    chat_id = args.chat_id
    if args.attach:
        client.send_media(chat_id, media_path=args.attach, caption=message)
    elif message:
        client.send_text(chat_id, message=message)


if __name__ == '__main__':
    main()
