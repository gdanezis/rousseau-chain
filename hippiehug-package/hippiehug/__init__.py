
VERSION = "0.0.2"

from hashlib import sha256

def h(item):
    ''' Returns the hash of an item. '''
    return sha256(item).digest()

class Leaf:

    __slots__ = ["item", "hid"]

    def __init__(self, item):
        self.item = item
        """ The item stored in the Leaf. """

        self.hid = h("L"+self.item)

    def identity(self):
        """ Returns the hash ID of the Leaf. """
        return self.hid

    def add(self, store, item):

        # Make a new leaf & store in DB
        l = Leaf(item)
        leaf_id = l.identity()
        store[leaf_id] = l

        # Only add once
        if item == self.item:
            return self

        # Add the new branch
        if self.item < item:
            b = Branch(self.item, self.identity(), leaf_id)
        else:
            b = Branch(item, leaf_id, self.identity())

        store[b.identity()] = b
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

    def evidence(self, store, evidence, item):
        return evidence + [ self ]

def _check_hash(key, val):
    if key != val.identity():
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

        self.hid = h("B" + self.pivot + self.left_branch + self.right_branch)

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

        store[b.identity()] = b
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

        b = Branch(self.pivot, new_b_left.identity(), new_b_right.identity())
        store[b.identity()] = b
        return b

    def is_in(self, store, item):
        if item <= self.pivot:
            return store[self.left_branch].is_in(store, item)
        else:   
            return store[self.right_branch].is_in(store, item)


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


class Tree:
    def __init__(self, store = {}, root_hash = None):
        """ Initiates a Merkle tree from a store and a root hash. 

        Example:
            >>> from hippiehug import Tree
            >>> t = Tree()
            >>> t.add("Hello")
            >>> "Hello" in t
            True
            >>> "World" not in t
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
                >>> t.multi_add(["Hello", "World"])
                >>> assert "Hello" in t and "World" in t

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

    def __contains__(self, item):
        return self.is_in(item)

    def evidence(self, item):
        """ Gathers evidence about the inclusion / exclusion of the *item*. 

        The evidence includes all Branches and Leafs necessary to prove the *item* is, 
        or is not, in the Merkle Tree. They are ordered from the root to the Leaf
        that either contrains the sought *item*, or not.

        Example:
            >>> t = Tree()
            >>> t.add("Hello")
            >>> t.add("World")
            >>> root, E = t.evidence("World")
            >>> evidence_store = dict((e.identity(), e) for e in E)
            >>> t2 = Tree(evidence_store, root)
            >>> "World" in t2
            True

        """
        if self.head == None:
            return []

        key = h(item)
        head_element = self.store[self.head]
        return self.head, head_element.evidence(self.store, [], key)


try:
    import redis
    import msgpack
except:
    print("Cannot load redis or msgpack")

def default(obj):
    """ Serialize objects using msgpack. """
    if isinstance(obj, Leaf):
        return msgpack.ExtType(42, obj.item)
    if isinstance(obj, Branch):
        datab = msgpack.packb((obj.pivot, obj.left_branch, obj.right_branch))
        return msgpack.ExtType(43,  datab)

    raise TypeError("Unknown Type: %r" % (obj,))


def ext_hook(code, data):
    """ Deserialize objects using msgpack. """
    if code == 42:
        return Leaf(data) 
    if code == 43:
        piv, r_leaf, l_leaf = msgpack.unpackb(data)
        return Branch(piv, r_leaf, l_leaf)

    return ExtType(code, data)


class RedisStore():
    def __init__(self, host="localhost", port=6379, db=0):
        """ Initialize a Redis backed store for the Merkle Tree. """
        self.r = redis.StrictRedis(host=host, port=port, db=db)
        self.cache = {}

    def __getitem__(self, key):
        if key in self.cache:
            return self.cache[key]
        
        if len(self.cache) > 10000:
            self.cache = {} 

        bdata = self.r.get(key)
        branch = msgpack.unpackb(bdata, ext_hook=ext_hook)
        # assert key == branch.identity()
        self.cache[key] = branch
        return branch

    def __setitem__(self, key, value):
        if key in self.cache:
            return

        bdata = msgpack.packb(value, default=default)
        # assert key == value.identity()
        self.r.set(key, bdata)

