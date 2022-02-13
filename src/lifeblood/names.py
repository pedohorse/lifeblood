import re

from lifeblood.logging import get_logger
from typing import Iterable


def match(match_expr: str, items: Iterable[str]):
    """
    return items matching match_expr
    TODO: speed it up
    :param match_expr:
    :param items:
    :return:
    """
    get_logger('scheduler').warning('this function is deprecated')
    res = []

    parts = []
    escaping = False
    in_quotes = False
    in_part = False
    offstart = 0
    part = ''
    for offset, symbol in enumerate(match_expr):
        if symbol in (' ', '\t'):
            if in_part and not escaping and not in_quotes:
                parts.append(part + match_expr[offstart:offset])
                in_part = False
            escaping = False
        else:
            if not in_part:
                in_part = True
                offstart = offset
                part = ''
            if symbol == '"' and not escaping:
                in_quotes = not in_quotes
                part += match_expr[offstart:offset]
                offstart = offset + 1
            if symbol == '\\' and not escaping:
                escaping = not escaping
                part += match_expr[offstart:offset]
                offstart = offset + 1
            else:
                escaping = False
    if in_part:
        parts.append(part + match_expr[offstart:])

    #
    #

    for item in items:
        good = False
        for part in parts:
            if part == '*':  # TODO: this is a special case
                good = True
            elif part[0] == '^':
                if part[1:] == item:
                    good = False
            else:
                if part == item:
                    good = True

        if good:
            res.append(item)

    return res
