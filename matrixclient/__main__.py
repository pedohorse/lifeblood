import sys
import os
import mimetypes
import io
import argparse
import shutil
import subprocess
import json
import tempfile
from matrix_client.client import MatrixClient


def _generate_preview_and_metadata(filepath):
    if shutil.which('ffprobe') is None or shutil.which('ffmpeg') is None:
        return None, None, None

    p = subprocess.Popen(['ffprobe', '-v', 'error', '-select_streams', 'v:0', '-show_entries', 'stream=width,height,r_frame_rate,duration',
                                  '-of', 'json=c=1',
                                  filepath], stdout=subprocess.PIPE, text=True)
    output, _ = p.communicate()
    retcode = p.poll()
    if retcode != 0:
        return None, None, None

    data = json.loads(output)
    data = data['streams'][0]

    fd, path = tempfile.mkstemp('.jpg', 'preview')
    subprocess.Popen(['ffmpeg', '-i', filepath, '-frames:v', '1', '-q:v', '2', '-y', path]).wait()

    return data, fd, path


def main(argv):
    parser = argparse.ArgumentParser(description='simple matrix notifier')
    commands = parser.add_subparsers(dest='command', help='what to do')
    
    com_send = commands.add_parser('send', help='send message to a room')
    com_send.add_argument('server', help='server to use')
    com_send.add_argument('token', help='matrix session token')
    com_send.add_argument('room', help='matrix room id')
    com_send.add_argument('message', nargs='?', help='message to send')
    com_send.add_argument('--message-is-file', action='store_true', help='treat message as file path to upload')

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
        if args.message_is_file:
            filepath = args.message
            if not os.path.exists(filepath):
                parser.error('file does not exist')
            filename = os.path.basename(filepath)
            mimetype = mimetypes.guess_type(filepath)[0] or ''
            with open(filepath, 'rb') as f:
                uri = c.upload(f.read(), mimetype, filename)
            if '/' in mimetype and mimetype.split('/', 1)[0] == 'image' or mimetype == 'image':
                r.send_image(uri, filename)
            elif '/' in mimetype and mimetype.split('/', 1)[0] == 'video' or mimetype == 'video':
                metadata, fd, preview_path = _generate_preview_and_metadata(filepath)
                extra_args = {}
                if metadata is not None:
                    if 'width' in metadata:
                        extra_args['w'] = metadata['width']
                    if 'height' in metadata:
                        extra_args['h'] = metadata['height']

                    with open(preview_path, 'rb') as f:
                        thumbnail_mxc = c.upload(f.read(), 'image/jpeg', preview_path)
                    extra_args['thumbnail_url'] = thumbnail_mxc
                r.send_video(uri, filename, **extra_args)
            else:
                r.send_file(uri, filename)
        else:
            if args.message is None:
                input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='UTF-8')
                args.message = input_stream.read()
            r.send_text(args.message)
    else:
        parser.print_help()
        parser.error('command need to be provided')


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]) or 0)
