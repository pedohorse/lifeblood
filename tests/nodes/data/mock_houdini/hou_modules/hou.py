import json
from pathlib import Path
from unittest.mock import MagicMock

_frame = 1
_bad_frames = set()
_default_output = None


class NodeError(RuntimeError):
    pass


class _NodeMock:
    def __init__(self, path: str, parms=None):
        self.__parms = parms
        self.__path = path

    def render(self, frame_range=None, *args, **kwargs):
        print(f'[MOCK-HOU] called Node.render', frame_range, args, kwargs)
        if frame_range is None:
            if _frame in _bad_frames:
                raise NodeError('GLOBAL BAD FRAME!')
            self.__render_log(_frame)
            return
        elif len(frame_range) == 2:
            start, end = frame_range
            inc = 1
        elif len(frame_range) == 3:
            start, end, inc = frame_range

        frame = start
        while frame <= end:
            if frame in _bad_frames:
                raise NodeError('GLOBAL BAD FRAME!')
            self.__render_log(frame)
            frame += inc

    def __render_log(self, frame):
        if _default_output is None:
            return
        with open(_default_output / 'render_log', 'a') as f:
            f.write(f'{self.__path} ::: {frame}\n')

    def __getattr__(self, item):
        print(f'[MOCK-HOU] called Node.{item}')
        return MagicMock()


class hipFile:
    @staticmethod
    def load(path, *args, **kwargs):
        # TODO: here we can load test script instead of hip
        print('[MOCK-HOU] load file:', path, args, kwargs)
        with open(path, 'r') as f:
            data = json.load(f)
        global _bad_frames, _default_output
        if bad_frames := data.get('bad_frames'):
            _bad_frames = set(bad_frames)
        if path := data.get('default_output'):
            _default_output = Path(path)

    @staticmethod
    def setName(*args, **kwargs):
        print('[MOCK-HOU] set name:', args, kwargs)

    @staticmethod
    def addEventCallback(*args, **kwargs):
        print('[MOCK-HOU] add Event Callback:', args, kwargs)


class takes:
    @staticmethod
    def currentTake():
        print('[MOCK-HOU] get current take')
        return MagicMock()

    @staticmethod
    def findTake(*args, **kwargs):
        print('[MOCK-HOU] find take', args, kwargs)
        return MagicMock()

    @staticmethod
    def setCurrentTake(*args, **kwargs):
        print('[MOCK-HOU] set current take', args, kwargs)


def node(path):
    print('[MOCK-HOU] get node:', path)
    return _NodeMock(path)


def setFrame(frame):
    print('[MOCK-HOU] set frame', frame)
    _frame = frame


def getFrame():
    return _frame


def setContextOption(name, val):
    print('[MOCK-HOU] set context option', name, val)
