import pytest

def test_basic():
    from hippiehug import Tree
    t = Tree()
    t.add(b"Hello")
    assert b"Hello" in t
    assert b"World" not in t

def test_evidence():
    from hippiehug import Tree
    t = Tree()
    t.add(b"Hello")
    t.add(b"World")

    root, E = t.evidence(b"World")
    assert root == t.root()
    store = dict((e.identity(), e) for e in E)
    t2 = Tree(store, root)
    assert b"World" in t2

@pytest.mark.skip(reason="no redis")
def test_store():
    import redis
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    r.flushdb()

    from hippiehug import Tree, RedisStore
    r = RedisStore(host="localhost", port=6379, db=0)
    t = Tree(store = r) 

    t.add(b"Hello")
    assert b"Hello" in t

def test_chain():
    from hippiehug import DocChain
    c = DocChain()
    c.multi_add([b"Hello", b"World"])

    # Test inclusion
    assert c.get(0, 0) == b"Hello"
    assert c.get(0, 0) != b"World"

    # Generate proof
    r = c.root()
    proof = {}
    c.get(0, 0, evidence=proof)

    # Test chain
    c2 = DocChain(store = proof, root_hash = r)
    assert c2.check(r, 0,0, b"Hello")

