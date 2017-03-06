

from hashlib import sha256 as xhash
def h(item):
    ''' Returns the hash of an item. '''
    return xhash(item).digest()

class Leaf:

    __slots__ = ["key", "item", "hid"]

    def __init__(self, item, key):
        self.item = item
        """ The item stored in the Leaf. """

        assert key is not None
        self.key = key
        """ The key under which the item is stored in the leaf. """

        #if key:
        #    self.key = key
        #else:
        #    self.key = item

        self.hid = h(b"L|" + self.key +b"|" + self.item)

    def identity(self):
        """ Returns the hash ID of the Leaf. """
        return self.hid

    def add(self, store, item, key):

        assert key is not None

        # Make a new leaf & store in DB
        l = Leaf(item, key)
        leaf_id = l.hid # l.identity()
        store[leaf_id] = l

        # Only add once
        if l.key == self.key:
            return self

        # Add the new branch
        if self.key <= l.key:
            b = Branch(self.key, self.hid, leaf_id)
        else:
            b = Branch(l.key, leaf_id, self.hid)

        store[b.hid] = b
        return b

    def multi_add(self, store, items, keys):
        assert keys is not None

        if items == []:
            return self

        # Add the first element to the store
        i = items[0]
        k = keys[0]
        b = self.add(store, i, key=k)

        # Skip if there is nothing left
        rest = items[1:]
        if rest == []:
            return b

        # Add the rest
        return b.multi_add(store, rest, keys=keys[1:])


    def is_in(self, store, item, key):
        assert key is not None

        l = Leaf(item, key)
        return l.hid == self.hid

    def lookup(self, store, key):
        if key == self.key:
            return (self.key, self.item)

        raise Exception("Key %s not found" % key)

    def evidence(self, store, evidence, key):
        return evidence + [ self ]

def _check_hash(key, val):
    if key != val.hid: # val.identity():
        raise Exception("Value has the wrong hash.")

class Branch:

    __slots__ = ["pivot", "left_branch", "right_branch", "hid", "key"]

    def __init__(self, pivot, left_branch_id, right_branch_id):
        self.pivot = pivot
        "The pivot element which determines the left and right leafs."

        self.left_branch = left_branch_id
        "The hash ID of the left leaf."

        self.right_branch = right_branch_id
        "The hash ID of the right leaf."

        self.hid = h(b"B" + self.pivot + self.left_branch + self.right_branch)
        self.key = self.hid

    def identity(self):
        """ Returns the hash ID of the Branch. """
        return self.hid

    def add(self, store, item, key):
        assert key is not None

        if key <= self.pivot:
            b_left = store[self.left_branch]
            _check_hash(self.left_branch, b_left)

            new_b_left = b_left.add(store, item, key)
            b = Branch(self.pivot, new_b_left.hid, self.right_branch)
            
        else:
            b_right = store[self.right_branch]
            _check_hash(self.right_branch, b_right)

            new_b_right = b_right.add(store, item, key)
            b = Branch(self.pivot, self.left_branch, new_b_right.hid)

        # store[b.identity()] = b
        store[b.hid] = b
        return b

    def multi_add(self, store, items, keys):
        if items == []:
            return self

        assert keys is not None

        left_list = []
        right_list = []
        left_keys = []
        right_keys = []

        for i, k in zip(items, keys):
            if k <= self.pivot:
                left_list += [ i ]
                left_keys += [ k ]
            else:
                right_list += [ i ]
                right_keys += [ k ]

        b_left = store[self.left_branch]
        if left_list != []:
            _check_hash(self.left_branch, b_left)
            new_b_left = b_left.multi_add(store, left_list, left_keys)
        else:
            new_b_left = b_left

        b_right = store[self.right_branch]
        if right_list != []:
            _check_hash(self.right_branch, b_right)
            new_b_right = b_right.multi_add(store, right_list, right_keys)
        else:
            new_b_right = b_right

        # b = Branch(self.pivot, new_b_left.identity(), new_b_right.identity())
        b = Branch(self.pivot, new_b_left.hid, new_b_right.hid)
        # store[b.identity()] = b
        store[b.hid] = b
        return b

    def lookup(self, store, key):
        if key <= self.pivot:
            return store[self.left_branch].lookup(store, key)
        else:   
            return store[self.right_branch].lookup(store, key)


    def is_in(self, store, item, key):
        assert key is not None

        if key <= self.pivot:
            return store[self.left_branch].is_in(store, item, key)
        else:   
            return store[self.right_branch].is_in(store, item, key)


    def multi_is_in_fast(self, store, evidence, items, keys, solution={}):
        if items == []:
            return

        assert keys is not None
        
        assert len(items) == len(keys)
        work_list = [(self, items, keys)]

        while work_list != []:

            (work_node, work_items, work_keys) = work_list.pop()
            
            if evidence is not None:
                evidence.append( work_node )

            if isinstance(work_node, Leaf):
                for i, k in zip(work_items, work_keys):
                    l = Leaf(i, k)
                    solution[(i, k)] = (l.hid == work_node.hid)
            else:
                left_list = []
                left_keys = []
                right_list = []
                right_keys = []

                assert len(work_items) == len(work_keys)
                for i, k in zip(work_items, work_keys):
                    if k <= work_node.pivot:
                        left_list += [ i ]
                        left_keys += [ k ]
                        assert len(left_list) == len(left_keys)
                    else:
                        right_list += [ i ]
                        right_keys += [ k ]
                        assert len(right_list) == len(right_keys)

                assert len(work_items) > 0
                assert len(left_list) + len(right_list) == len(work_items)

                b_left = store[work_node.left_branch]
                if left_list != []:
                    _check_hash(work_node.left_branch, b_left)
                    work_list.append( (b_left, left_list, left_keys) )
                    
                b_right = store[work_node.right_branch]
                if right_list != []:
                    _check_hash(work_node.right_branch, b_right)
                    work_list.append( (b_right, right_list, right_keys) )


    def evidence(self, store, evidence, key):
        evidence = evidence + [ self ]
        if key <= self.pivot:
            return store[self.left_branch].evidence(store, evidence, key)
        else:   
            return store[self.right_branch].evidence(store, evidence, key)

    def check(self, store):
        assert self.left_branch in store
        assert self.right_branch in store
        assert self.identity() in store

        try:
            assert store[self.left_branch].item <= self.pivot
        except:
            assert store[self.left_branch].pivot <= self.pivot
            store[self.left_branch].check(store)

        try:
            assert store[self.right_branch].item > self.pivot
        except:
            assert store[self.right_branch].pivot > self.pivot
            store[self.right_branch].check(store)



