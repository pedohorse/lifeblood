import urllib3
from pathlib import Path
import json


from typing import Union


class TelegramClient:
    def __init__(self, bot_token: str):
        self.__token = bot_token
        self.__headers = {'Content-Type': 'application/json'}
        self.__http = urllib3.PoolManager()

    @classmethod
    def __check_common_reply(cls, reply) -> dict:
        if (code := reply.status) != 200:
            raise RuntimeError(f'telegram responded with code {code}')

        data = reply.json()
        if not data.get('ok', False):
            raise RuntimeError(f'telegram api returned error: {data.get("description", "?")}')

        return data

    def send_text(self, chat_id: str, message: str, parse_mode: str = ''):
        result = self.__http.request(
            'POST',
            f'https://api.telegram.org/bot{self.__token}/sendMessage',
            headers=self.__headers,
            body=json.dumps({
                'chat_id': chat_id,
                'text': message,
                'parse_mode': parse_mode,
            }).encode('utf-8')
        )

        self.__check_common_reply(result)

    def send_media(self, chat_id: str, media_path: Union[Path, str], caption: str = '', parse_mode: str = ''):
        if isinstance(media_path, str):
            media_path = Path(media_path)

        if media_path.suffix in ('.gif',):
            return self.send_animation(chat_id, media_path, caption, parse_mode)
        elif media_path.suffix in ('.mp4',):
            return self.send_video(chat_id, media_path, caption, parse_mode)
        elif media_path.suffix in ('.bmp', '.jpg', '.jpeg', '.png',):
            return self.send_image(chat_id, media_path, caption, parse_mode)
        else:
            return self.send_document(chat_id, media_path, caption, parse_mode)

    def send_image(self, chat_id: str, image_path: Union[Path, str], caption: str = '', parse_mode: str = ''):
        if isinstance(image_path, str):
            image_path = Path(image_path)

        result = urllib3.request(
            'POST',
            f'https://api.telegram.org/bot{self.__token}/sendPhoto',
            # no json headers
            fields={
                'chat_id': chat_id,
                'caption': caption,
                'parse_mode': parse_mode,
                'photo': (f'preview{image_path.suffix}', image_path.read_bytes())
            }
        )

        data = self.__check_common_reply(result)

        return data.get('photo', [{}])[-1].get('file_id', None)

    def send_video(self, chat_id: str, video_path: Union[Path, str], caption: str = '', parse_mode: str = ''):
        if isinstance(video_path, str):
            video_path = Path(video_path)

        result = urllib3.request(
            'POST',
            f'https://api.telegram.org/bot{self.__token}/sendVideo',
            # no json headers
            fields={
                'chat_id': chat_id,
                'caption': caption,
                'parse_mode': parse_mode,
                'video': (f'preview{video_path.suffix}', video_path.read_bytes())
            }
        )

        data = self.__check_common_reply(result)

        return data.get('video', {}).get('file_id', None)

    def send_animation(self, chat_id: str, video_path: Union[Path, str], caption: str = '', parse_mode: str = ''):
        if isinstance(video_path, str):
            video_path = Path(video_path)

        result = urllib3.request(
            'POST',
            f'https://api.telegram.org/bot{self.__token}/sendAnimation',
            # no json headers
            fields={
                'chat_id': chat_id,
                'caption': caption,
                'parse_mode': parse_mode,
                'animation': (f'preview{video_path.suffix}', video_path.read_bytes())
            }
        )

        data = self.__check_common_reply(result)

        return data.get('animation', {}).get('file_id', None)

    def send_document(self, chat_id: str, file_path: Union[Path, str], caption: str = '', parse_mode: str = ''):
        if isinstance(file_path, str):
            file_path = Path(file_path)

        result = urllib3.request(
            'POST',
            f'https://api.telegram.org/bot{self.__token}/sendDocument',
            # no json headers
            fields={
                'chat_id': chat_id,
                'caption': caption,
                'parse_mode': parse_mode,
                'document': (f'preview{file_path.suffix}', file_path.read_bytes())
            }
        )

        data = self.__check_common_reply(result)

        return data.get('document', {}).get('file_id', None)

    def get_me(self):
        result = self.__http.request(
            'POST',
            f'https://api.telegram.org/bot{self.__token}/getUpdates',
            headers=self.__headers,
            body=b'{}',
        )

        return self.__check_common_reply(result)
