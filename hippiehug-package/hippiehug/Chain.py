from copy import copy

from Nodes import h, Leaf, Branch
from Tree import Tree

try:
    from msgpack import packb
except:
    print("No msgpack")

def get_fingers(seq):
    return set(seq - 1 - ((seq - 1) % (2**f)) for f in range(64))

def check_hash(key, val):
    if key != val.hid: # val.identity():
        raise Exception("Value has the wrong hash.")


class Block:
    def __init__(self, items, sequence=0, fingers=[]):
        """ Initialize a block. """
        self.sequence = sequence
        self.items = items  
        self.fingers = fingers

        # Precomute the head
        self.hid = self.head()

    def head(self):
        """ Returns the head of the block. """
        return h(packb(("S", self.sequence, self.fingers, self.items)))

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

        if evidence != None:
            evidence[self.hid] = self

        if block_seq == self.sequence:
            return self.items[item_seq]

        _, target_h = [(f,block_hash) for (f, block_hash) in self.fingers if f >= block_seq][-1]

        target_block = store[target_h]
        return target_block.get_item(store, block_seq, item_seq, evidence)



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

        last_block = self.store[self.head]
        return last_block.get_item(self.store, block_index, item_index, evidence)


