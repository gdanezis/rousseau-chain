# This is a convergence simulation for gossip based consensus.

import json
import time

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
	def __init__(self, start = [], quorum=1, name = None, shard=None):
		self.transactions = {}

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

		self.quiet = False

		if shard is None:
			self.shard = ["0"*64, "f"*64]
		else:
			self.shard = shard


	def _within_ID(self, idx):
		return self.shard[0] <= idx < self.shard[1]


	def _within_TX(self, Tx):
		## Tests whether a transaction is related to this node in 
		## any way. If not there is no case for processing it.
		idx, deps, outs = Tx
		if self._within_ID(idx):
			return True

		if any(self._within_ID(d) for d in deps):
			return True

		if any(self._within_ID(d) for d in outs):
			return True

		return False


	def gossip_towards(self, other_node):
		for k, v in self.pending_vote.iteritems():
			other_node.pending_vote[k] |= v

		# Should we process votes again here?
		other_node.commit_yes |= self.commit_yes
		other_node.commit_no |= self.commit_no
		assert other_node.commit_yes & other_node.commit_no == set()

		# other_node.commit_available |= self.commit_available
		other_node.commit_used |= self.commit_used


	def on_vote(self, full_tx, vote):
		pass


	def on_commit(self, full_tx, yesno):
		pass


	def process(self, Tx):

		if not self._within_TX(Tx):
			return

		# Cache the transaction
		self.transactions[Tx[0]] = Tx

		# Process the transaction
		if not self.quiet:
			print Tx[0]
		x = True
		while(x):
			x = self._process(Tx)


	def do_commit_yes(self, Tx):
		idx, deps, new_obj = Tx
		self.commit_yes.add(idx)
		self.pending_available |= set(new_obj) ## Add new transactions here
		self.commit_used |= set(deps)


	def _process(self, Tx):

		if not self._within_TX(Tx):
			return False

		idx, deps, new_obj = Tx
		all_deps = set(deps)
		deps = {d for d in deps if self._within_ID(d)}
		new_obj = set(new_obj) # By construction no repeats & fresh names

		if (idx in self.commit_yes or idx in self.commit_no):
			# Do not process twice
			if not self.quiet:
				print "Pass already decided"
			return False # No further progress can be made

		else:
			if deps & self.commit_used != set():
				
				# Some dependencies are used already!
				# So there is no way we will ever accept this
				# and neither will anyone else
				self.commit_no.add(idx)
				self.on_commit( Tx, False )

				if not self.quiet:
					print "Add to no"
				return False # there is no further work on this.

			# If we cannot exclude it out of hand then we kick in
			# the consensus protocol by considering it a candidate.

		xdeps = tuple(sorted(list(deps)))

		if not ( (self.name, xdeps, True) in self.pending_vote[idx] or (self.name, xdeps, False) in self.pending_vote[idx]):
			# We have not considered this as a pending candidate before
			# So now we have to vote on it.

			if deps.issubset(self.pending_available):
				# We have enough information on the transactions this
				# depends on, so we can vote.

				# Make a list of used transactions:
				used = { xd for xd, xtx in self.pending_used if xtx not in self.commit_no} 
				# and xd not in self.commit_used }
				## CHECK CORRECTNESS: Do we update on things that are eventually used?

				if set(deps) & used == set() and set(deps) & self.commit_used == set():
					# We cast a 'yes' vote -- since it seems that there
					# are no conflicts for this transaction in our pending list.

					self.pending_vote[idx].add( (self.name, xdeps, True) )
					self.pending_used |= set((d, idx) for d in deps)
					
					self.on_vote( Tx, (self.name, xdeps, True) )

					# TODO: add new transactions to available here
					#       Hm, actually we should not until it is confirmed.
					# self.pending_available |= new_obj ## Add new transactions here

					if not self.quiet:
						print "Pending yes"
					return True

				else:
					# We cast a 'no' vote since there is a conflict in our
					# history of transactions.
					self.pending_vote[idx].add( (self.name, xdeps, False) )

					self.on_vote( Tx, (self.name, xdeps, False) )

					if not self.quiet:
						print "Pending no"
					return True
			else:
				if not self.quiet:
					print "We know nothing about prerequisites. Continue ..."
				# We continue in case voting helps move things. This
				# happens in case others know about this transaction.

		if self.shard[0] <= idx < self.shard[1] or deps != set():
			# Only process the final votes if we are in charde of this
			# shard for the transaction or any dependencies.

			Votes = Counter()
			for oname, odeps, ovote in self.pending_vote[idx]:
				for d in odeps:
					Votes.update( [(d, ovote)] )

			yes_vote = all( Votes[(d, True)] >= self.quorum for d in all_deps )
			no_vote = any( Votes[(d, False)] >= self.quorum for d in all_deps )

			## Time to count votes for this transaction
			if yes_vote: # Counter(x for _,x in self.pending_vote[idx])[True] >= self.quorum:
				# We have a Quorum for including the transaction. So we update
				# all the committed state monotonically.
				self.do_commit_yes(Tx)

				self.on_commit( Tx, True )

				## CHECK CORRECT: Should I add the used transactions to self.pending_used?
				if not self.quiet:
					print "Commit yes"
				return False

			if no_vote: #Counter(x for _,x in self.pending_vote[idx])[False] >= self.quorum:
				# So sad: there is a quorum for rejecting this transaction
				# so we will now add it to the 'no' bucket.
				# Optional TODO: invalidate in the pending lists 
				self.commit_no.add(idx)

				self.on_commit( Tx, False )
				if not self.quiet:
					print "Commit no"
				return False

		return False # No further work


class MockNode(Node):

	def set_send(self, sender):
		self.send = sender

	def receive(self, message):
		# Ignore messages we sent
		if self.name == message["from"]:
			return

		tx = message['Tx']
		if not self._within_TX(tx):
			return

		if message['action'] == "vote":
			vote = message['vote']
			if vote not in self.pending_vote[tx[0]]:
				self.pending_vote[tx[0]].add( vote )
				self.process(tx)
	
		if message['action'] == "commit":
			idx = tx[0]
			if message["yesno"] == False:
				self.commit_no.add(idx)
			else:
				self.do_commit_yes(tx)

			# if not (idx in self.commit_yes or idx in self.commit_no):
			#	self.process(tx)

	def on_vote(self, full_tx, vote):
		msg = { "action":"vote", "from":self.name, "Tx":full_tx, "vote":vote }
		self.send(msg)


	def on_commit(self, full_tx, yesno):
		msg = { "action":"commit", "from":self.name, "Tx":full_tx, "yesno":yesno }
		self.send(msg)

def test_shard_many():
	limits = sorted([hexlify(urandom(32)) for _ in range(100)])
	limits = ["0" * 64] + limits + ["f" * 64]

	pre = ["444", "ccc", "ddd"]
	nodes = [MockNode(pre, 1, name="n%s" % i, shard=[b0,b1]) for i, (b0, b1) in enumerate(zip(limits[:-1],limits[1:]))]

	def send(msg):
		print "Send: " + str(msg)
		tx = msg["Tx"]
		ns = [n for n in nodes if n._within_TX(tx)]
		for n in ns:
			n.receive(msg)

	for n in nodes:
		n.set_send(send)

	T1 = ("333", ["444", "ccc"], [])
	T2 = ("bbb", ["444", "ddd"], [])


	n1 = [n for n in nodes if n._within_TX(T1)]
	n2 = [n for n in nodes if n._within_TX(T2)]

	# assert len(n1) == 3 and len(n2) == 3

	for n in n1:
		n.process(T1)

	for n in n2:
		n.process(T2)		
