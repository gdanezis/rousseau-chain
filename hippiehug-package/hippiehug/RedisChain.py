# We implement a chain that lives on Amazon S3
# For tests it necessary to have a configured AWS account.

import future

from .Chain import DocChain, Document, Block, ascii_hash
import redis

# from json import dumps, loads

from msgpack import packb, unpackb

from queue import Queue
from threading import Thread

class RedisChain():
    def __init__(self, chain_name, host='localhost', port=6379, db=0):
        """ Initialize the Redis chain with an redis database. """
        self.r = redis.StrictRedis(host, port, db)

        self.name = chain_name
        self.cache = {}

        # Recover the latest head, if there is one
        new_head = self.r.get('%s.head' % self.name)

        # Initialize the chain
        self.chain = DocChain(store=self, root_hash=new_head)


    def root(self):
        """ Returns the root of the chain. """
        return self.chain.root()


    def __getitem__(self, key):
        if key in self.cache:
            return self.cache[key]
        
        if len(self.cache) > 10000:
            self.cache = {} 

        o = unpackb(self.r.get(key))
        
        if o[b"type"] == b"Document":
            obj = Document(o[b"body"])

        if o[b"type"] == b"Block":
            obj = Block(items=o[b"items"], sequence=o[b"sequence"], fingers=o[b"fingers"])

        self.cache[key] = obj
        return obj


    def __setitem__(self, key, value):
        if key in self.cache:
            return
        else:
            self.cache[key] = value

        if isinstance(value, Document):
            o = packb({b"type":b"Document", b"body":value.item, b"hid":value.hid})
            self.r.set(key, o)

        if isinstance(value, Block):
            o = packb({b"type":b"Block", b"fingers":value.fingers, b"items":value.items, "sequence": value.sequence, "hid":value.hid})
            self.r.set(key, o)


    def add(self, items):
        # Pipelining for adding blocks
        pipe = self.r.pipeline()

        """ Add a new block with the given items. """
        self.chain.multi_add(items)

        # Only commit the new head after everything else.
        new_root = self.chain.root()
        self.r.set('%s.head' % self.name, new_root)

        # Execute the full pipeline
        pipe.execute()

    def get(self, bid, sid, evidence = None):
        """ Get the item at the block bid, position sid. Optionally, gather
        evidence for the proof."""
        return self.chain.get(bid, sid, evidence)


# ## ====================================================
# ## -------------------- TESTS -------------------------


def test_init():
    rc = RedisChain(b"test1")


def test_get_set():
    rc = RedisChain(b"test1")

    d = Document(b"Hello")
    rc[d.hid] = d
    rc.cache = {}

    d2 = rc[d.hid]
    assert d == d2

    b = Block([b"Hello", b"World"])
    rc[b.hid] = b
    rc.cache = {}

    b2 = rc[b.hid]
    assert b == b2


def test_create_add():

    rc = RedisChain(b"test3")
    rc.add([b"Hello1",b"World2"])

    assert rc.get(0,0) == b"Hello1"

    evidence = {}
    rc.get(0,0, evidence)

    d = DocChain(evidence, rc.root())
    assert d.get(0,0) == b"Hello1"
