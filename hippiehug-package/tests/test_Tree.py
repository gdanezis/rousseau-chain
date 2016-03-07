from hippiehug import RedisStore, Tree, Leaf, Branch
import redis


## ============== TESTS ===================


def test_evidence():
	t = Tree()

	# Test positive case
	t.add("Hello")
	t.add("World")

	root, E = t.evidence("World")
	assert len(E) == 2

	store = dict((e.identity(), e) for e in E)
	t2 = Tree(store, root)
	assert t2.is_in("World")

def _flushDB():
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    r.flushdb()

def test_store():
	_flushDB()

	r = RedisStore()

	l = Leaf("Hello")
	r[l.identity()] = l
	assert r[l.identity()].identity() == l.identity()

def test_store_tree():
	_flushDB()

	r = RedisStore()
	t = Tree(store = r)	

	from os import urandom
	for _ in range(100):
		item = urandom(32)
		t.add(item)
		assert t.is_in(item)
		assert not t.is_in(urandom(32))

def test_leaf_isin():
	l = Leaf("Hello")
	store = {l.identity() : l}
	b = l.add(store, "World")
	assert l.is_in(store, "Hello")


def test_Branch_isin():
	l = Leaf("Hello")
	store = {l.identity() : l}
	b = l.add(store, "World")
	assert b.is_in(store, "Hello")
	assert b.is_in(store, "World")

def test_Branch_multi():
	l = Leaf("Hello")
	store = {l.identity() : l}
	b = l.multi_add(store, ["B", "C"])
	b.check(store)

	assert b.is_in(store, "B")
	assert b.is_in(store, "C")
	assert b.is_in(store, "Hello")

def test_Branch_add():
	l = Leaf("Hello")
	store = {l.identity() : l}
	b = l.add(store, "World")

	b2 = b.add(store, "Doom")
	assert isinstance(b2, Branch)

	assert b2.left_branch in store
	assert b2.right_branch in store
	assert b2.identity() in store

	b2.check(store)

def test_add_like_a_monkey():
	
	root = Leaf("Hello")
	store = {root.identity() : root}

	from os import urandom
	for _ in range(100):
		item = urandom(32)
		root = root.add(store, item)
		root.check(store)
		assert root.is_in(store, item)

def test_Leaf_add():
	l = Leaf("Hello")
	store = {l.identity() : l}

	b = l.add(store, "World")

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
	t.add("Hello")
	assert t.is_in("Hello") == True

	# Infix operator
	assert "Hello" in t

def test_fail_isin():
	t = Tree()

	# Test negative case
	assert t.is_in("World") == False

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

	t.multi_add(["Hello", "World"])
	assert "Hello" in t		
	assert "World" in t

	t.multi_add(["A", "B", "C", "D", "E", "F"])
	assert "E" in t		
	assert "F" in t

def test_multi_test():
	t = Tree()	

	t.multi_add(["Hello", "World"])
	assert t.multi_is_in(["Hello", "World"]) == [True, True]

	answer, head, evidence = t.multi_is_in(["Hello", "World"], True)
	assert answer == [True, True]

	e = dict((k.identity(), k) for k in evidence)
	t2 = Tree(e, head)
	assert t2.multi_is_in(["Hello", "World"]) == [True, True]




