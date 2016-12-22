from .Nodes import h, Leaf, Branch

class Tree:
    def __init__(self, store = {}, root_hash = None):
        """ Initiates a Merkle tree from a store and a root hash. 

        Example:
            >>> from hippiehug import Tree
            >>> t = Tree()
            >>> t.add(b"Hello")
            >>> b"Hello" in t
            True
            >>> b"World" not in t
            True

        """
        self.head = root_hash
        self.store = store  ## This is our remote (key -> value) store

    def root(self):
        """ Returns the root of the Tree. Keep this value safe, and the integrity 
        of the set is guaranteed. """
        return self.head

    def add(self, item):
        """ Add and element to the Merkle tree. """
        key = h(item)
        if self.head == None:
            l = Leaf(key)
            self.store[l.identity()] = l
            self.head = l.identity()
        else:
            head_element = self.store[self.head]
            new_head_elem = head_element.add(self.store, key)
            self.head = new_head_elem.identity()
    
    def multi_add(self, items):
        """ Add many elements to the Merkle tree. This is 
        more efficient than adding individual elements.

        Example:
            >>> t = Tree()
            >>> t.multi_add([b"Hello", b"World"])
            >>> assert b"Hello" in t and b"World" in t

        """
        keys = [h(i) for i in items]

        if self.head == None:
            l = Leaf(keys[0])
            self.store[l.identity()] = l

            b = l.multi_add(self.store, keys[1:])
            self.head = b.identity()

        else:
            head_element = self.store[self.head]
            new_head_elem = head_element.multi_add(self.store, keys)
            self.head = new_head_elem.identity()


    def is_in(self, item):
        """ Checks whether an element is in the Merkle Tree. """
        if self.head == None:
            return False

        key = h(item)
        head_element = self.store[self.head]
        return head_element.is_in(self.store, key)

    def multi_is_in(self, items, evidence = False):
        """ Checks whether the items are in the Tree. Optionally, returns the 
        current head of the Tree and a list of Branches and Leafs as evidence. 

        Example lookup:
            >>> t = Tree()
            >>> t.multi_add([b"Hello", b"World"])
            >>> t.multi_is_in([b"Hello", b"World", b"!"])
            [True, True, False]

        Example gathering of evidence:
            >>> _, head, bag = t.multi_is_in([b"Hello", b"World", b"!"], True)
            >>> new_store = dict((e.identity(), e) for e in bag)
            >>> new_t = Tree(new_store, head)
            >>> new_t.multi_is_in([b"Hello", b"World", b"!"])
            [True, True, False]

        """

        if self.head == None:
            if not evidence:
                return [ False ] * len(items)
            else:
                return [ False ] * len(items), None, []

        keys = [ h(i) for i in items ]
        head_element = self.store[self.head]
                
        evid = [] if evidence else None

        solution = {}
        head_element.multi_is_in_fast( self.store, evid, keys, solution)

        if not evidence:
            return [solution[i] for i in keys]
        else:
            return [solution[i] for i in keys], self.head, evid

    def __contains__(self, item):
        return self.is_in(item)

    def evidence(self, item):
        """ Gathers evidence about the inclusion / exclusion of the *item*. 

        The evidence includes all Branches and Leafs necessary to prove the *item* is, 
        or is not, in the Merkle Tree. They are ordered from the root to the Leaf
        that either contrains the sought *item*, or not.

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
        if self.head == None:
            return []

        key = h(item)
        head_element = self.store[self.head]
        return self.head, head_element.evidence(self.store, [], key)

