from copy import copy, deepcopy
from binascii import hexlify
from msgpack import packb

from hippiehug.Utils import binary_hash


def get_fingers(seq):
    return set(seq - 1 - ((seq - 1) % (2**f)) for f in range(64))


def check_hash(key, val):
    if key != val.hid: # val.identity():
        raise Exception("Value has the wrong hash.")


def sort_dicts(unsorted):
    if isinstance(unsorted, dict):
        values_sorted = {k: sort_dicts(v) for k,v in unsorted.items()}
        return sorted(values_sorted.items())
    if isinstance(unsorted, list):
        # do not sort lists. they do have a defined order already.
        # we need to sort their elements though.
        return [sort_dicts(e) for e in unsorted]
    else:
        return unsorted


class Document:
    __slots__ = ["item", "hid"]

    def __init__(self, item):
        self.item = item
        """ The item stored in the Leaf. """

        self.hid = binary_hash(packb(("D", self.item)))

    def identity(self):
        """ Returns the hash ID of the Leaf. """
        return self.hid

    def __eq__(self, other):
        return self.hid == other.hid


class Block:
    def __init__(self, items, index=0, fingers=None, aux=None):
        """Initialize a block."""
        self.items = deepcopy(items)
        self.index = index
        self.fingers = deepcopy(fingers) if fingers else []
        self.aux = deepcopy(aux)

    def hash(self):
        """Return the head of the block."""
        return binary_hash(packb(
                ("S", self.index, self.fingers, sort_dicts(self.items), self.aux)))

    @property
    def hid(self):
        return self.hash()

    def next_block(self, store, items, pre_commit_fn=None):
        """Build a subsequent block, sealing a list of transactions.

        :param store: Backend
        :param items: Block items
        :param pre_commit_fn: Function that gets called on the block before
                it gets committed to the chain.
        """
        new_index = self.index + 1
        new_fingers = [(self.index, self.hid)]

        finger_index = get_fingers(new_index)
        new_fingers += [f for f in self.fingers if f[0] in finger_index]

        new_b = Block(items, new_index, new_fingers)

        if pre_commit_fn is not None:
            pre_commit_fn(new_b)

        store[new_b.hid] = new_b
        return new_b

    def get_item(self, store, block_index, item_index, evidence=None):
        """Return an item from the chain at a specific block and item index.

        :param store: Backend
        :param block_index: Block index
        :param item_index: Item index
        :param evidence: If not None, return a bundle of evidence
        """
        # print "FIND: %s (%s, %s)" % (self.index, block_index, item_index)

        if not (0 <= block_index <= self.index):
            raise Exception("Block is beyond this chain head: must be 0 <= %s <= %s." % (block_index, self.index))

        if evidence != None:
            evidence[self.hid] = self

        if block_index == self.index:
            if not (0 <= item_index < len(self.items)):
               raise Exception("Item is beyond this Block: must be 0 <= %s <= %s." % (item_index, len(self.items)))

            return self.items[item_index]

        _, target_h = [(f,block_hash) for (f, block_hash) in self.fingers if f >= block_index][-1]

        # Get the target block and check its integrity
        target_block = store[target_h]
        check_hash(target_h, target_block)

        return target_block.get_item(store, block_index, item_index, evidence)

    def __eq__(self, other):
        return self.hid == other.hid


class Chain:
    def __init__(self, store=None, root_hash=None):
        """Initialize a chain backed by a store."""
        self.store = store if store is not None else {}
        self.head = root_hash

    def root(self):
        """Return the head of the chain."""
        return self.head

    def multi_add(self, items, pre_commit_fn=None):
        """Add a batch of elements and seal a new block."""
        if self.head is None:
            # Make a new one
            b0 = Block(items)
            if pre_commit_fn is not None:
                pre_commit_fn(b0)
            self.store[b0.hid] = b0
            self.head = b0.hid
        else:
            last_block = self.store[self.head]
            b1 = last_block.next_block(self.store, items,
                    pre_commit_fn=pre_commit_fn)
            self.head = b1.hid

    def get(self, block_index, item_index, evidence=None):
        """Return the record at a specific block and item index,
        and potentially a bundle of evidence. """
        if self.head is None:
            return None

        ## Get head block and check its integrity
        last_block = self.store[self.head]
        check_hash(self.head, last_block)

        return last_block.get_item(self.store, block_index, item_index, evidence)


class DocChain(Chain):
    """A chain that stores hashes of documents. Construct like a *Chain*."""

    def multi_add(self, items):
        """Add multiple items to seal a new block."""

        docs = list(map(Document, items))
        for d in docs:
            self.store[d.hid] = d

        docs_id = list(map(lambda d: d.hid, docs))
        Chain.multi_add(self, docs_id)

    def get(self, block_index, item_index, evidence=None):
        """Get a sealed item, and optionally a bundle of evidence."""

        ## Get Doc and check its hash
        item = Chain.get(self, block_index, item_index, evidence)
        d = self.store[item]
        check_hash(item, d)

        if evidence != None:
            evidence[d.hid] = d

        return self.store[item].item

    def check(self, root, block_index, item_index, item):
        """Check that an item is within the structure at a specific point."""
        ret = True
        ret = ret and (self.root() == root)
        ret = ret and (self.get(block_index, item_index) == item)
        return ret

