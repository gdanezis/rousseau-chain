# -*- coding: utf-8 -*-

import six
from hashlib import sha256 as xhash

from binascii import hexlify


def binary_hash(item):
    """
    >>> binary_hash(b'value')[:4]
    b'\xcdB@M'
    """
    return xhash(item).digest()


def ascii_hash(item):
    """
    >>> ascii_hash(b'value')[:4]
    b'cd42'
    """
    return hexlify(binary_hash(item))
