from hippiehug.Chain import Chain, Block, DocChain


def test_block_hash():
    store = {}
    b0 = Block( [b"item1", b"item2"], 0, [b"A", b"B"])
    store[b0.hid] = b0

    b1 = b0.next_block(store, [b"item3", b"item4"])


def test_block_duplicated():
    b0 = Block(items=[b"item1", b"item2"], index=0, fingers=[b"A", b"B"], aux=42)
    assert b0.aux == 42

    b1 = Block(b0.items, b0.index, b0.fingers, b0.aux)

    assert b0.items == b1.items
    assert b0.index == b1.index
    assert b0.fingers == b1.fingers
    assert b0.aux == b1.aux


def test_block_with_hash_duplicated():
    cc = {
        'mtr_hash': None,
        'metadata': {
            'params': {
                'dh_pk': 'VVREziRt5sorx9pqi5xpj16t1ibH1XPmNPqXBfCRNZD',
                'sig_pk': 'VVRF1XiSmteSYFVkBdu9VY588CyPNKymZ6igm9xoEgN',
                'vrf_pk': 'VVRF2rC2EFimdovJ5yhSx3BLpKsTy7iMTnvsK6NDskB',
                'rescue_pk': 'VVREycH7jJqvhoEfP7ejAxfz1DAe5cKBUi1zet5cjnt'
            },
            'identity_info': "I'm VVREziRt5sorx9pqi5xpj16t1ibH1XPmNPqXBfCRNZD"
        },
        'version': 1,
        'nonce': 'Q3JTBfZr3Q1PZ1JRfx3KNn',
        'timestamp': 1525089767.931872
    }
    orig = Block(items=[cc], index=0, fingers=[], aux=42)
    copy = Block(orig.items, orig.index, orig.fingers, orig.aux)
    cc['timestamp'] = 1525089767.931234
    changed_value = Block([cc], orig.index, orig.fingers, orig.aux)
    assert orig.hid == copy.hid
    assert orig.hid != changed_value.hid


def test_block_constructor_independence():
    items, fingers, aux = [], [], []
    b0 = Block(items, 0, fingers, aux)
    items.append(1)
    fingers.append(1)
    aux.append(1)
    assert b0.items == []
    assert b0.fingers == []
    assert b0.aux == []


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

def test_chain_pre_commit():
    c = Chain()
    items = ["main"]

    def add_aux_data(block):
        block.aux = ["aux"]

    c.multi_add(items, pre_commit_fn=add_aux_data)
    block = c.store[c.head]

    assert block.items == ["main"]
    assert block.aux == ["aux"]
    assert block.hash() in c.store

def test_chain_default_store():
    c = Chain()
    c.multi_add(["test"])
    assert c.get(0, 0) is not None

    c2 = Chain()
    assert c2.get(0, 0) is None

def test_chain_empty_store():
    store = {}
    c = Chain(store)
    c.multi_add(["test"])
    assert c.get(0, 0) == "test"

    c2 = Chain(store, root_hash=c.head)
    assert c2.get(0, 0) == "test"

