
from hashlib import sha256

def h(item):
	''' Returns the hash of an item. '''
	return sha256(item).digest()

class Leaf:
	def __init__(self, item):
		self.item = item

	def identity(self):
		return h("L"+self.item)

	def add(self, store, item):
		# Make a new leaf & store in DB
		l = Leaf(item)
		leaf_id = l.identity()
		store[leaf_id] = l

		# Add the new branch
		if self.item < item:
			b = Branch(self.item, self.identity(), leaf_id)
		else:
			b = Branch(item, leaf_id, self.identity())

		store[b.identity()] = b
		return b

	def is_in(self, store, item):
		return item == self.item

class Branch:
	def __init__(self, pivot, left_branch_id, right_branch_id):
		self.pivot = pivot
		self.left_branch = left_branch_id
		self.right_branch = right_branch_id

	def identity(self):
		return h("B" + self.pivot + self.left_branch + self.right_branch)

	def add(self, store, item):
		if item < self.pivot:
			b_left = store[self.left_branch]
			new_b_left = b_left.add(store, item)

			b = Branch(self.pivot, new_b_left.identity(), self.right_branch)
			
		else:
			b_right = store[self.right_branch]
			new_b_right = b_right.add(store, item)

			b = Branch(self.pivot, self.left_branch, new_b_right.identity())

		store[b.identity()] = b
		return b

	def is_in(self, store, item):
		if item <= self.pivot:
			return store[self.left_branch].is_in(store, item)
		else:	
			return store[self.right_branch].is_in(store, item)


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
	def __init__(self):
		self.head = None
		self.store = { }  ## This is our remote (key -> value) store

	def add(self, item):
		key = h(item)
		if self.head == None:
			l = Leaf(key)
			self.store[l.identity()] = l
			self.head = l.identity()
		else:
			head_element = self.store[self.head]
			new_head_elem = head_element.add(self.store, key)
			self.head = new_head_elem.identity()

	def is_in(self, item):
		if self.head == None:
			return False

		key = h(item)
		head_element = self.store[self.head]
		return head_element.is_in(self.store, key)


## ============== TESTS ===================

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

def test_add_like_fucking_monkey():
	
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

		from binascii import hexlify
	print 
	print hexlify(t.head)


