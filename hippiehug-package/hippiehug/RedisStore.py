from .Nodes import h, Leaf, Branch

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

