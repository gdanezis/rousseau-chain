

from hashlib import sha256 as xhash
def h(item):
    ''' Returns the hash of an item. '''
    return xhash(item).digest()

class Leaf:

    __slots__ = ["item", "hid"]

    def __init__(self, item):
        self.item = item
        """ The item stored in the Leaf. """

        self.hid = h(b"L"+self.item)

    def identity(self):
        """ Returns the hash ID of the Leaf. """
        return self.hid

    def add(self, store, item):

        # Make a new leaf & store in DB
        l = Leaf(item)
        leaf_id = l.hid # l.identity()
        store[leaf_id] = l

        # Only add once
        if item == self.item:
            return self

        # Add the new branch
        if self.item < item:
            # b = Branch(self.item, self.identity(), leaf_id)
            b = Branch(self.item, self.hid, leaf_id)
        else:
            # b = Branch(item, leaf_id, self.identity())
            b = Branch(item, leaf_id, self.hid)

        # store[b.identity()] = b
        store[b.hid] = b
        return b

    def multi_add(self, store, items):
        if items == []:
            return self

        # Add the first element to the store
        i = items[0]
        b = self.add(store, i)

        # Skip if there is nothing left
        rest = items[1:]
        if rest == []:
            return b

        # Add the rest
        return b.multi_add(store, rest)


    def is_in(self, store, item):
        return item == self.item

    def multi_is_in(self, store, evidence, items, solution={}):
        if items == []:
            return

        if evidence is not None:
            evidence.append( self )

        for i in items:
            solution[i] = (i == self.item)

    def evidence(self, store, evidence, item):
        return evidence + [ self ]

def _check_hash(key, val):
    if key != val.hid: # val.identity():
        raise Exception("Value has the wrong hash.")

class Branch:

    __slots__ = ["pivot", "left_branch", "right_branch", "hid"]

    def __init__(self, pivot, left_branch_id, right_branch_id):
        self.pivot = pivot
        "The pivot element which determines the left and right leafs."

        self.left_branch = left_branch_id
        "The hash ID of the left leaf."

        self.right_branch = right_branch_id
        "The hash ID of the right leaf."

        self.hid = h(b"B" + self.pivot + self.left_branch + self.right_branch)

    def identity(self):
        """ Returns the hash ID of the Branch. """
        return self.hid

    def add(self, store, item):
        if item <= self.pivot:
            b_left = store[self.left_branch]
            _check_hash(self.left_branch, b_left)

            new_b_left = b_left.add(store, item)
            b = Branch(self.pivot, new_b_left.identity(), self.right_branch)
            
        else:
            b_right = store[self.right_branch]
            _check_hash(self.right_branch, b_right)

            new_b_right = b_right.add(store, item)
            b = Branch(self.pivot, self.left_branch, new_b_right.identity())

        # store[b.identity()] = b
        store[b.hid] = b
        return b

    def multi_add(self, store, items):
        if items == []:
            return self

        left_list = [i for i in items if i <= self.pivot]
        right_list = [i for i in items if i > self.pivot]

        b_left = store[self.left_branch]
        if left_list != []:
            _check_hash(self.left_branch, b_left)
            new_b_left = b_left.multi_add(store, left_list)
        else:
            new_b_left = b_left

        b_right = store[self.right_branch]
        if right_list != []:
            _check_hash(self.right_branch, b_right)
            new_b_right = b_right.multi_add(store, right_list)
        else:
            new_b_right = b_right

        # b = Branch(self.pivot, new_b_left.identity(), new_b_right.identity())
        b = Branch(self.pivot, new_b_left.hid, new_b_right.hid)
        # store[b.identity()] = b
        store[b.hid] = b
        return b

    def is_in(self, store, item):
        if item <= self.pivot:
            return store[self.left_branch].is_in(store, item)
        else:   
            return store[self.right_branch].is_in(store, item)

    def multi_is_in(self, store, evidence, items, solution={}):
        if items == []:
            return

        if evidence is not None:
            evidence.append( self )

        left_list = [i for i in items if i <= self.pivot]
        right_list = [i for i in items if i > self.pivot]

        b_left = store[self.left_branch]
        if left_list != []:
            _check_hash(self.left_branch, b_left)
            b_left.multi_is_in(store, evidence, left_list, solution)

        b_right = store[self.right_branch]
        if right_list != []:
            _check_hash(self.right_branch, b_right)
            b_right.multi_is_in(store, evidence, right_list, solution)

    def multi_is_in_fast(self, store, evidence, items, solution={}):
        if items == []:
            return

        work_list = [(self, items)]

        while work_list != []:

            (work_node, work_items) = work_list.pop()

            if evidence is not None:
                evidence.append( work_node )

            if isinstance(work_node, Leaf):
                for i in work_items:
                    solution[i] = (i == work_node.item)
            else:

                left_list = [i for i in work_items if i <= work_node.pivot]
                right_list = [i for i in work_items if i > work_node.pivot]

                b_left = store[work_node.left_branch]
                if left_list != []:
                    _check_hash(work_node.left_branch, b_left)
                    work_list.append( (b_left, left_list) )
                    
                b_right = store[work_node.right_branch]
                if right_list != []:
                    _check_hash(work_node.right_branch, b_right)
                    work_list.append( (b_right, right_list) )

    def evidence(self, store, evidence, item):
        evidence = evidence + [ self ]
        if item <= self.pivot:
            return store[self.left_branch].evidence(store, evidence, item)
        else:   
            return store[self.right_branch].evidence(store, evidence, item)

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



