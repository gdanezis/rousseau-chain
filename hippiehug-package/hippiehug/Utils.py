# -*- coding: utf-8 -*-

import six
from hashlib import sha256 as xhash

from binascii import hexlify


def binary_hash(item):
    """
    >>> isinstance(binary_hash(b'value')[:4], six.binary_type)
    True
    """
    return xhash(item).digest()


def ascii_hash(item):
    """
    >>> ascii_hash(b'value')[:4] == b'cd42'
    True
    """
    return hexlify(binary_hash(item))
