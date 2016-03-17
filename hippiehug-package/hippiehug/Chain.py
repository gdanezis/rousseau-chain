from copy import copy

from Nodes import h, Leaf, Branch
from Tree import Tree

from msgpack import packb

def get_fingers(seq):
    return set(seq - 1 - ((seq - 1) % (2**f)) for f in range(64))

class Block:
    def __init__(self, items, sequence=0, fingers=[]):
        self.sequence = sequence
        self.items = items  
        self.fingers = fingers

        # Precomute the head
        self.xhead = self.head()

    def head(self):
        return h(packb(("S", self.sequence, self.fingers, self.items)))

    def next_block(self, store, items):
        new_sequence = self.sequence + 1 
        new_fingers = [ (self.sequence, self.xhead) ]

        finger_index = get_fingers(new_sequence)
        new_fingers += [f for f in self.fingers if f[0] in finger_index]

        new_b = Block(items, new_sequence, new_fingers)
        store[new_b.xhead] = new_b
        return new_b

    def get_item(self, store, block_seq, item_seq, evidence = None):
        # print "FIND: %s (%s, %s)" % (self.sequence, block_seq, item_seq)

        if evidence != None:
            evidence[self.xhead] = self

        if block_seq == self.sequence:
            return self.items[item_seq]

        _, target_h = [(f,block_hash) for (f, block_hash) in self.fingers if f >= block_seq][-1]

        target_block = store[target_h]
        return target_block.get_item(store, block_seq, item_seq)



class Chain:
    def __init__(self, store = {}, root_hash = None):
        """ Initializes a chained backed by a store. """
        self.store = store
        self.head = root_hash

    def root(self):
        return self.head

    def multi_add(self, items):
        if self.head is None:
            # Make a new one
            b0 = Block( items )
            self.store[b0.xhead] = b0
            self.head = b0.xhead
        else:
            last_block = self.store[self.head]
            b1 = last_block.next_block(self.store, items)
            self.head = b1.xhead

    def get(self, block_index, item_index, evidence=False):
        if self.head is None:
            return None

        last_block = self.store[self.head]
        return last_block.get_item(self.store, block_index, item_index)


def test_block_hash():
    store = {}
    b0 = Block( ["item1", "item2"], 0, ["A", "B"])
    store[b0.xhead] = b0

    b1 = b0.next_block(store, ["item3", "item4"])

def test_block_find():
    store = {}
    b0 = Block( ["item1", "item2"], 0, [])
    store[b0.xhead] = b0

    for i in range(1, 99):
        item = [ "%s|%s" % (i,j) for j in range(100) ]
        assert len(item) == 100
        b0 = b0.next_block(store, item)

    res1 =  b0.get_item(store, 50, 30)
    assert res1 == "50|30"
    assert b0.get_item(store, 0, 1) == "item2"

def test_chain():
    vals = []
    c = Chain()
    for i in range(0, 99):
        vals += [ (i,j,"%s|%s" % (i,j)) for j in range(100)]

        items = [ "%s|%s" % (i,j) for j in range(100) ]
        c.multi_add(items)

    res1 =  c.get(50, 30)
    assert res1 == "50|30"
    assert c.get(0, 1) == "0|1"

    for i, j, v in vals:
        assert c.get(i, j) == v