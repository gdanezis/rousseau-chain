from hippiehug import RedisStore, Tree, Leaf, Branch
import pytest


## ============== TESTS ===================


def test_evidence():
     t = Tree()

     # Test positive case
     t.add(b"Hello", b"Hello")
     t.add(b"World", b"World")

     root, E = t.evidence(b"World")
     assert len(E) == 2

     store = dict((e.identity(), e) for e in E)
     t2 = Tree(store, root)
     assert t2.is_in(b"World")


def test_store(rstore):
    l = Leaf(b"Hello", b"Hello")
    rstore[l.identity()] = l
    assert rstore[l.identity()].identity() == l.identity()


def test_store_tree(rstore):
    t = Tree(store=rstore)

    from os import urandom
    for _ in range(100):
        item = urandom(32)
        t.add(item, item)
        assert t.is_in(item)
        assert not t.is_in(urandom(32))


def test_leaf_isin():
     l = Leaf(b"Hello", b"Hello")
     store = {l.identity() : l}
     b = l.add(store, b"Woitemrld", b"Woitemrld")
     assert l.is_in(store, b"Hello", b"Hello")


def test_leaf_isin_map():
     l = Leaf(item=b"Hello", key=b"World")
     store = {l.identity() : l}
     b = l.add(store, b"World", b"World")
     assert l.is_in(store, item=b"Hello", key=b"World")


def test_Branch_isin():
     l = Leaf(b"Hello", b"Hello")
     store = {l.identity() : l}
     b = l.add(store, b"World", b"World")
     assert b.is_in(store, b"Hello", b"Hello")
     assert b.is_in(store, b"World", b"World")

def test_Branch_isin_map():
     l = Leaf(item=b"Hello", key=b"A")
     store = {l.identity() : l}
     b = l.add(store, item=b"World", key=b"B")
     assert b.is_in(store, b"Hello", b"A")
     assert b.is_in(store, b"World", b"B")
     assert not b.is_in(store, b"World", b"C")

def test_Branch_multi():
     l = Leaf(b"Hello", b"Hello")
     store = {l.identity() : l}
     b = l.multi_add(store, [b"B", b"C"], [b"B", b"C"])
     b.check(store)

     assert b.is_in(store, b"B", b"B")
     assert b.is_in(store, b"C", b"C")
     assert b.is_in(store, b"Hello", b"Hello")

def test_Branch_add():
     l = Leaf(b"Hello", b"Hello")
     store = {l.identity() : l}
     b = l.add(store, b"World", b"World")

     b2 = b.add(store, b"Doom", b"Doom")
     assert isinstance(b2, Branch)

     assert b2.left_branch in store
     assert b2.right_branch in store
     assert b2.identity() in store

     b2.check(store)

def test_add_like_a_monkey():

     root = Leaf(b"Hello",b"Hello")
     store = {root.identity() : root}

     from os import urandom
     for _ in range(100):
          item = urandom(32)
          root = root.add(store, item, item)
          root.check(store)
          assert root.is_in(store, item, item)

def test_Leaf_add():
     l = Leaf(b"Hello", b"Hello")
     store = {l.identity() : l}

     b = l.add(store, b"World", b"World")

     assert isinstance(b, Branch)

     assert b.left_branch in store
     assert b.right_branch in store
     assert b.identity() in store

     assert store[b.left_branch].item <= b.pivot
     assert store[b.right_branch].item > b.pivot


def test_Tree():
     t = Tree()

def test_add_isin():
     t = Tree()

     # Test positive case
     t.add(b"Hello")
     assert t.is_in(b"Hello") == True

     # Infix operator
     assert b"Hello" in t

def test_fail_isin():
     t = Tree()

     # Test negative case
     assert t.is_in(b"World") == False

def test_massive():
     t = Tree()

     from os import urandom
     for _ in range(100):
          item = urandom(32)
          t.add(item)
          assert t.is_in(item)
          assert not t.is_in(urandom(32))

def test_multi_add():
     t = Tree()

     from os import urandom
     X = [urandom(32) for _ in range(100)]
     t.multi_add(X)

     for x in X:
          assert x in t

     X = [urandom(32) for _ in range(100)]
     t.multi_add(X)

     for x in X:
          assert x in t

     Y = [urandom(32) for _ in range(100)]
     for y in Y:
          assert y not in t

def test_multi_small():
     t = Tree()

     t.multi_add([b"Hello", b"World"])
     assert b"Hello" in t
     assert b"World" in t

     t.multi_add([b"A", b"B", b"C", b"D", b"E", b"F"])
     assert b"E" in t
     assert b"F" in t

def test_multi_test():
     t = Tree()

     t.multi_add([b"Hello", b"World"])
     assert t.multi_is_in([b"Hello", b"World"]) == [True, True]

     answer, head, evidence = t.multi_is_in([b"Hello", b"World"], evidence=True)
     assert answer == [True, True]

     e = dict((k.identity(), k) for k in evidence)
     t2 = Tree(e, head)
     assert t2.multi_is_in([b"Hello", b"World"]) == [True, True]

def test_lookup():
     l = Leaf(item=b"Hello", key=b"A")
     store = {l.identity() : l}
     b = l.add(store, item=b"World", key=b"B")
     assert b.is_in(store, b"Hello", b"A")
     assert b.is_in(store, b"World", b"B")
     assert not b.is_in(store, b"World", b"C")

     assert b.lookup(store, b"B") == (b"B", b"World")

     try:
          b.lookup(store, b"B") == (b"B", b"World2")
          assert False
     except:
          assert True

     try:
          b.lookup(store, b"C") == (b"B", b"World2")
          assert False
     except:
          assert True

def test_double_add():
     l = Leaf(item=b"Hello", key=b"A")
     store = {l.identity() : l}
     b = l.add(store, item=b"World", key=b"B")
     assert b.is_in(store, b"Hello", b"A")
     assert b.is_in(store, b"World", b"B")
     assert not b.is_in(store, b"World", b"C")

     b = b.add(store, item=b"World2", key=b"B")

     assert b.lookup(store, b"B") == (b"B", b"World")
     assert not b.lookup(store, b"B") == (b"B", b"World2")

def test_tree_default_store():
    t = Tree()
    t.multi_add([b"test"])
    assert t.is_in(b"test")

    t2 = Tree()
    assert not t2.is_in(b"test")

def test_tree_empty_store():
    store = {}
    t = Tree(store)
    t.multi_add([b"test"])
    assert t.is_in(b"test")

    t2 = Tree(store, root_hash=t.root())
    assert t2.is_in(b"test")
