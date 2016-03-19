# This is a convergence simulation for gossip based consensus.

import json
import time
import logging

from os import urandom
from random import sample, shuffle
from binascii import hexlify
from collections import defaultdict, Counter

from hashlib import sha256
from struct import pack

from consensus import Node, packageTx

class MockNode(Node):

	def set_send(self, sender):
		""" Set a custom network sender. """
		self.send = sender

	def receive(self, message):
		""" How to process incoming messages. """

		# Ignore messages we sent
		if self.name == message["from"]:
			return

		tx = message['Tx']
		idx, deps, new_obj, data = tx
		if not tx == packageTx(data, deps, len(new_obj)):
			raise Exception("Invalid transaction.")

		if not self._within_TX(tx):
			raise Exception("Transaction not of interest.")

		if message['action'] == "vote":
			vote = message['vote']
			if vote not in self.pending_vote[tx[0]]:
				self.pending_vote[tx[0]].add( vote )
				self.process(tx)
	
		if message['action'] == "commit":
			idx = tx[0]

			# if not (idx in self.commit_yes or idx in self.commit_no):
			#	self.process(tx)

			if message["yesno"] == False:
				self.commit_no.add(idx)
			else:
				self.do_commit_yes(tx)


	def on_vote(self, full_tx, vote):
		msg = { "action":"vote", "from":self.name, "Tx":full_tx, "vote":vote }
		self.send(msg)


	def on_commit(self, full_tx, yesno):
		msg = { "action":"commit", "from":self.name, "Tx":full_tx, "yesno":yesno }
		self.send(msg)


