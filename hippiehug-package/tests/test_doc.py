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

def test_chain():
    from hippiehug import DocChain
    c = DocChain()
    c.multi_add(["Hello", "World"])

    # Test inclusion
    assert c.get(0, 0) == "Hello"
    assert c.get(0, 0) != "World"

    # Generate proof
    r = c.root()
    proof = {}
    c.get(0, 0, evidence=proof)

    # Test chain
    c2 = DocChain(store = proof, root_hash = r)
    assert c2.check(r, 0,0, "Hello")


