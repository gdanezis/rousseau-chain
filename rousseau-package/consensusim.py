# This is a convergence simulation for gossip based consensus.

from os import urandom
from random import sample, shuffle
from binascii import hexlify
from collections import defaultdict, Counter

from hashlib import sha256
from struct import pack

def h(data):
	return hexlify(sha256(data).digest())

def packageTx(data, deps, num_out):
	hx = sha256(data)
	for d in sorted(deps):
		hx.update(d)

	actualID = hx.digest()
	actualID = actualID[:-2] + pack("H", 0)

	out = []
	for i in range(num_out):
		out.append(actualID[:-2] + pack("H", i+1))

	return (hexlify(actualID), sorted(deps), map(hexlify,out))

class Node:
	def __init__(self, start = [], quorum=1, name = None):
		self.quorum = quorum
		self.name = name if name is not None else urandom(16)
		self.pending_vote = defaultdict(set)
		#self.pending_no  = defaultdict(set)

		self.pending_available = set(start)
		self.pending_used = set()

		self.commit_yes  = set()
		self.commit_no   = set()
		# self.commit_available = set(start)
		self.commit_used = set()

	def gossip_towards(self, other_node):
		for k, v in self.pending_vote.iteritems():
			other_node.pending_vote[k] |= v

		# Should we process votes again here?
		other_node.commit_yes |= self.commit_yes
		other_node.commit_no |= self.commit_no
		assert other_node.commit_yes & other_node.commit_no == set()

		# other_node.commit_available |= self.commit_available
		other_node.commit_used |= self.commit_used


	def on_vote(self, vote):
		pass

	def on_commit(self, tx, yesno):
		pass

	def process(self, Tx):
		print Tx[0]
		x = True
		while(x):
			x = self._process(Tx)

	def _process(self, Tx):
		idx, deps, new_obj = Tx
		deps = set(deps)
		new_obj = set(new_obj) # By construction no repeats & fresh names

		if (idx in self.commit_yes or idx in self.commit_no):
			# Do not process twice
			print "Pass already decided"
			return False # No further progress can be made

		else:
			if deps & self.commit_used != set():
				
				# Some dependencies are used already!
				# So there is no way we will ever accept this
				# and neither will anyone else
				self.commit_no.add(idx)
				self.on_commit( idx, False )

				print "Add to no"
				return False # there is no further work on this.

			# If we cannot exclude it out of hand then we kick in
			# the consensus protocol by considering it a candidate.

		if not ( (self.name, True) in self.pending_vote[idx] or (self.name, False) in self.pending_vote[idx]):
			# We have not considered this as a pending candidate before
			# So now we have to vote on it.

			if deps.issubset(self.pending_available):
				# We have enough information on the transactions this
				# depends on, so we can vote.

				# Make a list of used transactions:
				used = {xd for xd, xtx in self.pending_used if xtx not in self.commit_no and xd not in self.commit_used}
				## CHECK CORRECTNESS: Do we update on things that are eventually used?

				if set(deps) & used == set() :
					# We cast a 'yes' vote -- since it seems that there
					# are no conflicts for this transaction in our pending list.

					self.pending_vote[idx].add( (self.name,True) )
					self.pending_used |= set((d, idx) for d in deps)
					
					self.on_vote( (self.name,True) )

					# TODO: add new transactions to available here
					#       Hm, actually we should not until it is confirmed.
					# self.pending_available |= new_obj ## Add new transactions here

					print "Pending yes"
					return True

				else:
					# We cast a 'no' vote since there is a conflict in our
					# history of transactions.
					self.pending_vote[idx].add( (self.name, False) )

					self.on_vote( (self.name, False) )

					print "Pending no"
					return True
			else:
				print "We know nothing about prerequisites. Continue ..."
				# We continue in case voting helps move things. This
				# happens in case others know about this transaction.

		## Time to count votes for this transaction
		if Counter(x for _,x in self.pending_vote[idx])[True] >= self.quorum:
			# We have a Quorum for including the transaction. So we update
			# all the committed state monotonically.
			self.commit_yes.add(idx)
			self.pending_available |= new_obj ## Add new transactions here
			self.commit_used |= set(deps)

			self.on_commit( idx, True )

			## CHECK CORRECT: Should I add the used transactions to self.pending_used?

			print "Commit yes"
			return False

		if Counter(x for _,x in self.pending_vote[idx])[False] >= self.quorum:
			# So sad: there is a quorum for rejecting this transaction
			# so we will now add it to the 'no' bucket.
			# Optional TODO: invalidate in the pending lists 
			self.commit_no.add(idx)

			self.on_commit( idx, False )

			print "Commit no"
			return False

		return False # No further work


def test_random():
	resources = [hexlify(urandom(16)) for _ in range(300)]
	transactions = [(hexlify(urandom(16)), sample(resources,2), []) for _ in range(300)]

	n = Node(resources, 2)
	shuffle(transactions)
	tx_list = sample(transactions, 100)
	for tx in transactions:
		n.process(tx)

	n2 = Node(resources,2)
	n.gossip_towards(n2)
	for tx in transactions:
		n2.process(tx)

def test_wellformed():
	import json

	resources = [hexlify(urandom(16)) for _ in range(300)]
	# def packageTx(data, deps, num_out)
	transactions = []
	for x in range(300):
		deps = sample(resources,2)
		data = json.dumps({"ID":x})
		tx = packageTx(data, deps, 2)
		transactions.append((tx, data))
	# [(hexlify(urandom(16)), sample(resources,2), []) for x in range(300)]

	n = Node(resources, 1)
	shuffle(transactions)
	tx_list = sample(transactions, 100)
	for tx, data in transactions:
		idx, deps, out = tx

		## First perform the Tx checks
		assert packageTx(data, deps, 2) == tx

		## Now process this transaction
		n.process(tx)
		
		for i, o in enumerate(out):
			print "Out%s: %s" % (i, o)



def test_small():
	T1 = ("T1", ["A", "B"], [])
	T2 = ("T2", ["B", "C"], [])

	n = Node(["A", "B", "C"],1)
	n.process(T1)
	n.process(T2)
	assert "T1" in n.commit_yes
	assert "T2" not in n.commit_yes

def test_small_chain():
	T1 = ("T1", ["A"], ["B"])
	T2 = ("T2", ["B"], ["C"])

	n = Node(["A"],1)
	n.process(T1)
	n.process(T2)
	assert "C" in n.pending_available

def test_chain_conflict():
	T1 = ("T1", ["A"], ["B"])
	T2 = ("T2", ["A"], ["C"])
	T3 = ("T3", ["B"], ["D"])
	T4 = ("T4", ["C"], ["F"])

	n = Node(["A"],1)
	for tx in [T1, T2, T3, T4]:
		n.process(tx)

def test_quorum_simple():
	T1 = ("T1", ["A", "B"], [])
	T2 = ("T2", ["B", "C"], [])

	n1 = Node(["A", "B", "C"], 2)
	n2 = Node(["A", "B", "C"], 2)
	n3 = Node(["A", "B", "C"], 2)

	n1.process(T1)
	n2.process(T2)
	n2.process(T1)
	n3.process(T1)

	n1.gossip_towards(n2)
	n3.gossip_towards(n2)

	n2.process(T1)
	assert "T1" in n2.commit_yes


