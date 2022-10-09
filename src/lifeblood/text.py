import random
import shlex
from fnmatch import fnmatchcase

from typing import Optional, List, Iterable


def generate_name(word_length: int = 6, max_word_length: Optional[int] = None):
    word_length = max(1, word_length)
    if max_word_length is not None:
        word_length = random.randint(word_length, max(word_length, max_word_length))

    blk1 = 'aeiouy'
    blk2 = 'bcdfghjklmnprstvw'

    if random.random() > 0.5:
        blk1, blk2 = blk2, blk1

    parts: List[str] = []
    for i in range(word_length):
        parts.append(random.choice(blk1 if i % 2 == 0 else blk2))

    parts[0] = parts[0].capitalize()
    return ''.join(parts)


def match_pattern(pattern: str, text: str) -> bool:
    """
    checks if text matches pattern, where pattern is a number of fnmatch patterns separated by whitespaces
    quotes can be used for patterns with spaces, as the whole line is treated as shell arguments, patterns are splitted using shlex.split
    so do not forget to escape quotes that are actually supposed to be quotes

    patterns can be patterns and antipatterns:
    if a pattern starts with ^ symbol - it is an antipattern.
    if you need an actual ^ symbol - you will have to double escape it - once for shlex to eat off, second time for the pattern itself

    if text matches ANY of patterns and doesn't match any antipatterns - it passes

    :param pattern:
    :param text:
    :return:
    """
    matched = False
    for subpattern in shlex.split(pattern):
        if matched and subpattern.startswith('^'):  # antipattern
            if fnmatchcase(text, subpattern[1:]):
                matched = False
        elif not matched:
            if subpattern.startswith(r'\^'):
                subpattern = subpattern[1:]
            if fnmatchcase(text, subpattern):
                matched = True
    return matched


def filter_by_pattern(pattern: str, items: Iterable[str]) -> List[str]:
    """
    takes a list of
    :param pattern: 
    :param items: 
    :return: 
    """
    return [x for x in items if match_pattern(pattern, x)]


def nice_memory_formatting(memory_bytes: int) -> str:
    prefix = ''
    if memory_bytes < 0:
        prefix = '-'
        memory_bytes *= -1
    suff = ('B', 'KB', 'MB', 'GB', 'TB', 'PB')
    next_suff = 'EB'
    for su in suff:
        if memory_bytes < 1100:
            return f'{prefix}{memory_bytes}{su}'
        memory_bytes //= 1000
    return f'{prefix}{memory_bytes}{next_suff}'
