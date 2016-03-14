import sys

import json
import time

from os import urandom
from random import sample, shuffle
from binascii import hexlify
from collections import defaultdict, Counter

from hashlib import sha256
from struct import pack

from consensusim import packageTx, Node, Timer

import cProfile, pstats, StringIO

if __name__ == "__main__":
	resources = [hexlify(urandom(16)) for _ in range(10000)]
	# def packageTx(data, deps, num_out)
	transactions = []
	for x in range(10000):
		deps = sample(resources,2)
		data = json.dumps({"ID":x})
		tx = packageTx(data, deps, 2)
		transactions.append((tx, data))
	# [(hexlify(urandom(16)), sample(resources,2), []) for x in range(300)]

	n = Node(resources, 1)
	n.quiet = True
	shuffle(transactions)
	# tx_list = sample(transactions, 100)


	if "-P" in sys.argv:
		pr = cProfile.Profile()
		pr.enable()

	with Timer() as t:
		for tx, data in transactions:
			idx, deps, out = tx

			## First perform the Tx checks
			assert packageTx(data, deps, 2) == tx

			## Now process this transaction
			n.process(tx)

	if "-P" in sys.argv:
		pr.disable()
		s = StringIO.StringIO()
		sortby = "tottime"
		ps = pstats.Stats(pr, stream=s)
		ps.strip_dirs()
		ps.sort_stats(sortby)
		ps.print_stats()
		print s.getvalue()


	print "Time taken: %2.2f sec" % (t.interval) 
	