#!/usr/bin/env python

import sys
import argparse
import random
import json
import re
import lifeblood_connection
from pathlib import Path
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from cgi import parse_multipart

from typing import Dict, List, Optional, Tuple


class DataAccessor:
    def has_data(self, key: str):
        raise NotImplementedError()

    def get_data(self, key: str) -> str:
        raise NotImplementedError()

    def set_data(self, key: str, data: str):
        raise NotImplementedError()

    def store_input_files(self, identifier: str, contents: List[bytes]) -> Tuple[str]:
        raise NotImplementedError()

    def set_output_files_future(self, identifier: str, future):
        raise NotImplementedError()

    def output_files_ready(self, identifier: str) -> bool:
        raise NotImplementedError()

    def get_output_files(self, identifier: str) -> Dict[str, bytes]:
        raise NotImplementedError()


class DictDataAccessor(DataAccessor):
    def __init__(self, storage_base_path: Path):
        self.__dict: Dict[str, str] = {}
        self.__futures = {}
        self.__results = {}
        self.__input_storage_base = storage_base_path

    def has_data(self, key: str):
        return key in self.__dict

    def get_data(self, key: str) -> str:
        return self.__dict[key]

    def set_data(self, key: str, data: str):
        self.__dict[key] = data

    def store_input_files(self, identifier: str, contentss: List[bytes]) -> Tuple[str]:
        if identifier is None:
            raise NotImplementedError()
        base_file_path = self.__input_storage_base / identifier
        base_file_path.mkdir(exist_ok=True, parents=True)
        file_paths = []
        for i, contents in enumerate(contentss):
            file_path = base_file_path / f'file_{i}'
            with open(file_path, 'wb') as f:
                f.write(contents)
            file_paths.append(str(file_path))
        return tuple(file_paths)

    def set_output_files_future(self, identifier: str, future):
        if identifier in self.__futures:
            raise RuntimeError(f'identifier {identifier} already has a future')
        self.__futures[identifier] = future

    def output_files_ready(self, identifier: str) -> bool:
        if identifier in self.__results:
            return True
        if identifier in self.__futures:
            return self.__futures[identifier].is_done()
        raise ValueError(f'identifier {identifier} is neither ready nor awaited')

    def get_output_files(self, identifier: str) -> Dict[str, bytes]:
        if identifier in self.__results:
            return self.__read_files(self.__results[identifier])
        if identifier in self.__futures:
            iid, raw_reply = self.__futures[identifier].wait()
            reply = json.loads(raw_reply.decode('UTF-8'))
            self.__results[identifier] = tuple(reply.get('files', ()))
            self.__futures.pop(identifier)
            return self.__read_files(self.__results[identifier])
        raise ValueError(f'identifier {identifier} is neither ready nor awaited')

    @classmethod
    def __read_files(cls, file_paths: Tuple[str]) -> Dict[str, bytes]:
        datas = {}
        for file_path in file_paths:
            with open(file_path, 'rb') as f:
                datas[file_path] = f.read()
        return datas


class Handler(BaseHTTPRequestHandler):
    def __init__(self, main_html: str, result_not_ready_html: str, result_ready_html: str, css: str, data_accessor: DataAccessor, expect_reply: bool, *args, **kwargs):
        self.__index_html = main_html
        self.__result_not_ready_html = result_not_ready_html
        self.__result_ready_html = result_ready_html
        self.__css = css
        self.__data_accessor = data_accessor
        self.__expect_reply = expect_reply
        super().__init__(*args, **kwargs)

    def _respond_not_found(self):
        self.send_response(404)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(b'<h1>YOU SHALL NOT PASS</h1>')

    def _respond_redirect(self, to: str):
        self.send_response(301)
        self.send_header('Location', to)
        self.end_headers()

    def do_GET(self):
        if self.path == '/':
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(self.__index_html.encode('utf-8'))
        elif self.path == '/styles.css':
            self.send_response(200)
            self.send_header("Content-type", "text/css")
            self.end_headers()
            self.wfile.write(self.__css.encode('utf-8'))
        elif self.path.startswith('/result/'):
            # if no expecting reply - just show empty result page
            if not self.__expect_reply:
                self.send_response(200)
                self.send_header("Content-type", "text/html")
                body = self.__result_ready_html.format(result='')
                self.end_headers()
                self.wfile.write(body.encode('utf-8'))
                return
            #
            # otherwise we wait for result to be ready
            key = self.path[len('/result/'):]
            file_key = None
            if key.count('/') > 1:
                self._respond_not_found()
                return
            elif '/' in key:
                key, file_key = key.split('/', 1)

            try:
                result_ready = self.__data_accessor.output_files_ready(key)
            except ValueError:
                self._respond_not_found()
                return
            if result_ready:
                files = self.__data_accessor.get_output_files(key)
                keys_ordered = tuple(files.keys())
                self.send_response(200)
                if file_key is None:
                    self.send_header("Content-type", "text/html")
                    body = self.__result_ready_html.format(
                        result='\n'.join(f'<br><a href="/result/{key}/{i}" download="{Path(path).name}">{Path(path).name}</a>' for i, path in enumerate(keys_ordered))
                    )
                    self.end_headers()
                    self.wfile.write(body.encode('utf-8'))
                else:
                    # file key is just order in default sorted file dict
                    key_file_path = keys_ordered[int(file_key)]
                    data = files[key_file_path]
                    file_name = Path(key_file_path).name.replace('"', '_')
                    self.send_header('Content-Disposition', f'attachment; filename=\"{file_name}\"')
                    self.send_header('Content-Length', f'{len(data)}')
                    self.end_headers()
                    self.wfile.write(data)

                return
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(self.__result_not_ready_html.format(key=key, rand=random.random()).encode('utf-8'))
        else:
            self._respond_not_found()

    def do_POST(self):
        if self.path != '/do_something_smart':
            self._respond_not_found()
            return

        files = parse_multipart(self.rfile, {k.strip(): v.strip().encode('utf-8') for part in
                                             self.headers['Content-Type'].split(';')[1:] for k, v in
                                             (part.split('=', 1),)})
        images = files.get('images')
        if not images:
            self._respond_not_found()

        key = lifeblood_connection.generate_addressee()

        file_paths = self.__data_accessor.store_input_files(key, images)

        attribs = {
            'files': file_paths,
        }
        if self.__expect_reply:
            attribs.update({
                'server_iid': lifeblood_connection.get_my_invocation_id(),
                'server_addressee': key,
            })

        lifeblood_connection.create_task('file processing', attribs, blocking=True)
        self.__data_accessor.set_output_files_future(key, lifeblood_connection.message_to_invocation_receive(key, blocking=False))

        self._respond_redirect(f'/result/{key}')


def main(args, base_path: Path):
    parser = argparse.ArgumentParser(description='run simplest web server for internal network use')
    parser.add_argument('--port', '-p', type=int, help='port to open server on')
    parser.add_argument('--ip', '-a', type=str, help='address the server will listen to')
    parser.add_argument('--title', type=str, help='page welcome text')
    parser.add_argument('--expect-reply', action='store_true', help='page welcome text')
    parser.add_argument('--base_storage_path', type=str, help='where to store input files')
    opts = parser.parse_args(args)

    # html+css based on https://codepen.io/Scribblerockerz/pen/qdWzJw
    with open(base_path/"index.html") as f:
        html_body = f.read()
    html_body = html_body.format(title=opts.title or '<strong>File upload</strong> example',
                                 header_title=re.sub(r'<([^<>]+)>(.*?)<\/\1>', r'\2', opts.title or 'File upload'))

    with open(base_path/"styles.css") as f:
        css_body = f.read()

    with open(base_path/"result_not_ready.html") as f:
        result_not_ready_html_template = f.read()

    with open(base_path/"result_ready.html") as f:
        result_ready_html_template = f.read()

    data_accessor = DictDataAccessor(Path(opts.base_storage_path))

    server = ThreadingHTTPServer(
        (opts.ip or '127.0.0.1', opts.port),
        lambda *args, **kwargs: Handler(html_body, result_not_ready_html_template, result_ready_html_template, css_body, data_accessor, opts.expect_reply, *args, **kwargs)
    )
    server.serve_forever()


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:], Path(sys.argv[0]).parent) or 0)
