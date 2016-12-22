from copy import copy
from binascii import hexlify

from .Nodes import h as binary_hash

def ascii_hash(s):
    return hexlify(binary_hash(s)[:20])

try:
    from msgpack import packb
except:
    print("No msgpack")

def get_fingers(seq):
    return set(seq - 1 - ((seq - 1) % (2**f)) for f in range(64))

def check_hash(key, val):
    if key != val.hid: # val.identity():
        raise Exception("Value has the wrong hash.")

class Document:
    __slots__ = ["item", "hid"]

    def __init__(self, item):
        self.item = item
        """ The item stored in the Leaf. """

        self.hid = ascii_hash(packb(("D",self.item)))

    def identity(self):
        """ Returns the hash ID of the Leaf. """
        return self.hid

    def __eq__(self, other):
        return self.hid == other.hid

class Block:
    def __init__(self, items, sequence=0, fingers=[]):
        """ Initialize a block. """
        self.sequence = sequence
        self.items = items  
        self.fingers = fingers
        self.length = len(items)

        # Precomute the head
        self.hid = self.head()

    def head(self):
        """ Returns the head of the block. """
        return ascii_hash(packb(("S", self.sequence, self.fingers, self.length, self.items)))

    def next_block(self, store, items):
        """ Builds a subsequent block, selaing a list of transactions. """
        new_sequence = self.sequence + 1 
        new_fingers = [ (self.sequence, self.hid) ]

        finger_index = get_fingers(new_sequence)
        new_fingers += [f for f in self.fingers if f[0] in finger_index]

        new_b = Block(items, new_sequence, new_fingers)
        store[new_b.hid] = new_b
        return new_b

    def get_item(self, store, block_seq, item_seq, evidence = None):
        """ Returns an iten from the chain, at a specific block and item ID.
        Optionally returns a bundle of evidence. """
        # print "FIND: %s (%s, %s)" % (self.sequence, block_seq, item_seq)

        if not (0 <= block_seq <= self.sequence):
            raise Exception("Block is beyond this chain head: must be 0 <= %s <= %s." % (block_seq, self.sequence))

        if evidence != None:
            evidence[self.hid] = self

        if block_seq == self.sequence:

            if not (0 <= item_seq < self.length):
               raise Exception("Item is beyond this Block: must be 0 <= %s <= %s." % (item_seq, self.length))

            return self.items[item_seq]

        _, target_h = [(f,block_hash) for (f, block_hash) in self.fingers if f >= block_seq][-1]

        # Get the target block and check its integrity
        target_block = store[target_h]
        check_hash(target_h, target_block)

        return target_block.get_item(store, block_seq, item_seq, evidence)

    def __eq__(self, other):
        return self.hid == other.hid


class Chain:
    def __init__(self, store = {}, root_hash = None):
        """ Initializes a chained backed by a store. """
        self.store = store
        self.head = root_hash

    def root(self):
        """ Returns the head of the chain. """
        return self.head

    def multi_add(self, items):
        """ Adds a batch of elements and seals a new block. """
        if self.head is None:
            # Make a new one
            b0 = Block( items )
            self.store[b0.hid] = b0
            self.head = b0.hid
        else:
            last_block = self.store[self.head]
            b1 = last_block.next_block(self.store, items)
            self.head = b1.hid

    def get(self, block_index, item_index, evidence=None):
        """ Returns the record at a specific block an item ID, 
            and potentially a bundle of evidence. """
        if self.head is None:
            return None

        ## Get head block and check its integrity
        last_block = self.store[self.head]
        check_hash(self.head, last_block)

        return last_block.get_item(self.store, block_index, item_index, evidence)

class DocChain(Chain):
    ''' A chain that stores hashes of documents. Construct like a *Chain*. '''

    def multi_add(self, items):
        ''' Add multiple items to seal a new block. '''

        docs = list(map(Document, items))
        for d in docs:
            self.store[d.hid] = d

        docs_id = list(map(lambda d: d.hid, docs))
        Chain.multi_add(self, docs_id)

    def get(self, block_index, item_index, evidence=None):
        ''' Get a sealed item, and optionally a bundle of evidence. '''

        ## Get Doc and check its hash
        item = Chain.get(self, block_index, item_index, evidence)
        d = self.store[item]
        check_hash(item, d)
        
        if evidence != None:
            evidence[d.hid] = d

        return self.store[item].item

    def check(self, root, block_index, item_index, item):
        """ Check that an item is within the structure at a specific point. """
        ret = True
        ret = ret and (self.root() == root)
        ret = ret and (self.get(block_index, item_index) == item)
        return ret

