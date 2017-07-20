from .Nodes import h, Leaf, Branch


class Tree:
    def __init__(self, store=None, root_hash=None):
        """Initialize a Merkle tree from a store and a root hash.

        Example:
            >>> from hippiehug import Tree
            >>> t = Tree()
            >>> t.add(b"Hello")
            >>> b"Hello" in t
            True
            >>> b"World" not in t
            True

        """
        self.store = store if store is not None else {}
        self.root_hash = root_hash

    def root(self):
        """Return the root of the Tree.

        Keep this value safe, and the integrity of the set is guaranteed.
        """
        return self.root_hash

    def add(self, item, key=None):
        """Add and element to the Merkle tree."""
        item_key = h(item)

        if key is None:
            key = item

        if self.root_hash == None:
            l = Leaf(item_key, key)
            self.store[l.identity()] = l
            self.root_hash = l.identity()
        else:
            head_element = self.store[self.root_hash]
            new_head_elem = head_element.add(self.store, item_key, key)
            self.root_hash = new_head_elem.identity()

    def multi_add(self, items, keys=None):
        """Add many elements to the Merkle tree.

        This is more efficient than adding individual elements.

        :param items: Items to add
        :param keys:

        Example:
            >>> t = Tree()
            >>> t.multi_add([b"Hello", b"World"])
            >>> assert b"Hello" in t and b"World" in t
        """

        item_keys = [h(i) for i in items]
        if keys is None:
            keys = items

        if self.root_hash == None:
            l = Leaf(item_keys[0], keys[0])
            self.store[l.identity()] = l

            b = l.multi_add(self.store, item_keys[1:], keys[1:])
            self.root_hash = b.identity()

        else:
            head_element = self.store[self.root_hash]
            new_head_elem = head_element.multi_add(self.store, item_keys, keys)
            self.root_hash = new_head_elem.identity()

    def is_in(self, item, key=None):
        """Checks whether an element is in the Merkle Tree.

        :param item: Item to check
        :param key: If not None, hash of the item is used as a lookup key
        """
        if self.root_hash == None:
            return False

        if key is None:
            key = item

        item_key = h(item)
        head_element = self.store[self.root_hash]
        return head_element.is_in(self.store, item_key, key)

    def multi_is_in(self, items, keys=None, evidence=False):
        """Check whether the items are in the Tree.

        :param items: Items to check
        :param keys: If not None, hashes of items are used as lookup keys
        :param evidence: Return the current root_hash of the Tree and a list
                of Branches and Leafs as evidence.

        Example lookup:
            >>> t = Tree()
            >>> t.multi_add([b"Hello", b"World"])
            >>> t.multi_is_in([b"Hello", b"World", b"!"])
            [True, True, False]

        Example gathering of evidence:
            >>> _, root_hash, bag = t.multi_is_in([b"Hello", b"World", b"!"], evidence=True)
            >>> new_store = dict((e.identity(), e) for e in bag)
            >>> new_t = Tree(new_store, root_hash)
            >>> new_t.multi_is_in([b"Hello", b"World", b"!"])
            [True, True, False]

        Example using key-values:
            >>> t = Tree()
            >>> t.add(key=b"K1", item=b"V1")
            >>> t.add(key=b"K2", item=b"V2")
            >>> t.is_in(key=b"K2", item=b"V2")
            True
            >>> t.is_in(key=b"K1", item=b"V1")
            True
            >>> t.multi_is_in(keys=[b"K2", b"K1", b"K2", b"!"], items=[b"V2", b"V1", b"!", b"V2"])
            [True, True, False, False]

        """

        # >>> t.multi_add(keys=[b"K1", b"K2"], items=[b"V1", b"V2"])

        if keys is None:
            keys = items

        if self.root_hash == None:
            if not evidence:
                return [ False ] * len(items)
            else:
                return [ False ] * len(items), None, []

        item_keys = [ h(i) for i in items ]
        head_element = self.store[self.root_hash]

        evid = [] if evidence else None

        solution = {}
        head_element.multi_is_in_fast( self.store, evid, items=item_keys, keys=keys, solution=solution)

        if not evidence:
            return [solution[(i, k)] for i, k in zip(item_keys, keys)]
        else:
            return [solution[(i, k)] for i, k in zip(item_keys, keys)], self.root_hash, evid

    def __contains__(self, item):
        return self.is_in(item)

    def evidence(self, key=None):
        """Gather evidence about the inclusion / exclusion of the *key*.

        The evidence includes all Branches and Leafs necessary to prove the *key* is,
        or is not, in the Merkle Tree. They are ordered from the root to the Leaf
        that either contrains the sought *key*, or not.

        Example:
            >>> t = Tree()
            >>> t.add(b"Hello")
            >>> t.add(b"World")
            >>> root, E = t.evidence(b"World")
            >>> evidence_store = dict((e.identity(), e) for e in E)
            >>> t2 = Tree(evidence_store, root)
            >>> b"World" in t2
            True

        """
        if self.root_hash == None:
            return []

        # item_key = h(item)
        head_element = self.store[self.root_hash]
        return self.root_hash, head_element.evidence(self.store, [], key)

