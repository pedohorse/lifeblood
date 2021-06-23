import random

from typing import List, Optional


def generate_name(word_length: int = 6, max_word_length: Optional[int] = None):
    word_length = max(1, word_length)
    if max_word_length is not None:
        word_length = random.randint(word_length, max_word_length)

    blk1 = 'aeiouy'
    blk2 = 'bcdfghjklmnprstvw'

    if random.random() > 0.5:
        blk1, blk2 = blk2, blk1

    parts: List[str] = []
    for i in range(word_length):
        parts.append(random.choice(blk1 if i % 2 == 0 else blk2))

    parts[0] = parts[0].capitalize()
    return ''.join(parts)
