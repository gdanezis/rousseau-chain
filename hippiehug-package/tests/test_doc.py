def test_basic():
    from hippiehug import Tree
    t = Tree()
    t.add("Hello")
    assert "Hello" in t
    assert "World" not in t

def test_evidence():
    from hippiehug import Tree
    t = Tree()
    t.add("Hello")
    t.add("World")

    root, E = t.evidence("World")
    assert root == t.root()
    store = dict((e.identity(), e) for e in E)
    t2 = Tree(store, root)
    assert "World" in t2

def test_store():
    import redis
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    r.flushdb()

    from hippiehug import Tree, RedisStore
    r = RedisStore(host="localhost", port=6379, db=0)
    t = Tree(store = r) 

    t.add("Hello")
    assert "Hello" in t

