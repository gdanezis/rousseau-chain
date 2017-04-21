from hippiehug.Chain import Chain, Block, DocChain


def test_block_hash():
    store = {}
    b0 = Block( [b"item1", b"item2"], 0, [b"A", b"B"])
    store[b0.hid] = b0

    b1 = b0.next_block(store, [b"item3", b"item4"])

def test_block_find():
    store = {}
    b0 = Block( [b"item1", b"item2"], 0, [])
    store[b0.hid] = b0

    for i in range(1, 99):
        item = list(map(lambda x: x.encode(),[ "%s|%s" % (i,j) for j in range(100) ]))
        assert len(item) == 100
        b0 = b0.next_block(store, item)

    res1 =  b0.get_item(store, 50, 30)
    assert res1 == b"50|30"
    assert b0.get_item(store, 0, 1) == b"item2"

def test_chain():
    vals = []
    c = Chain()
    for i in range(0, 99):
        vals += [ (i,j,("%s|%s" % (i,j)).encode()) for j in range(100)]

        items = list(map(lambda x: x.encode(),[ "%s|%s" % (i,j) for j in range(100) ]))
        c.multi_add(items)

    res1 =  c.get(50, 30)
    assert res1 == b"50|30"
    assert c.get(0, 1) == b"0|1"

    for i, j, v in vals:
        assert c.get(i, j) == v

def test_chain_evidence():
    c = Chain()
    for i in range(0, 99):
        items = list(map(lambda x: x.encode(),[ "%s|%s" % (i,j) for j in range(100) ]))
        c.multi_add(items)

    evidence = {}
    res1 =  c.get(50, 30, evidence)

    c2 = Chain(evidence, root_hash = c.head)
    assert c2.get(50, 30) == b"50|30"

def test_chain_doc():
    c = DocChain()
    for i in range(0, 99):
        items = list(map(lambda x: x.encode(),[ "%s|%s" % (i,j) for j in range(100) ]))
        c.multi_add(items)

    evidence = {}
    res1 =  c.get(50, 30, evidence)

    c2 = DocChain(evidence, root_hash = c.head)
    assert c2.get(50, 30) == b"50|30"
    assert c2.check(c.root(), 50, 30, b"50|30")

import pytest

def test_chain_negative():
    c = Chain()
    for i in range(0, 99):
        items = list(map(lambda x: x.encode(),[ "%s|%s" % (i,j) for j in range(100) ]))
        c.multi_add(items)

    with pytest.raises(Exception) as IX:
        assert c.get(150, 30) == b"50|30"
    
    with pytest.raises(Exception) as IX:
        assert c.get(50, 130) == b"50|30"