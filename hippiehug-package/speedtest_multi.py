import cProfile
from hippiehug import Tree
import StringIO
import pstats

import time

class Timer:    
    def __enter__(self):
        self.start = time.clock()
        return self

    def __exit__(self, *args):
        self.end = time.clock()
        self.interval = self.end - self.start

def main():
	t = Tree()	

	from os import urandom
	rep = 100000
	print("For %s repetitions:" % rep)
	X = [str(x) for x in xrange(rep)]
	bulk = ["x" + str(x) for x in xrange(rep)]
	t.multi_add(bulk)


	with Timer() as tim:
		t.multi_add(X)

	print "Time per add: %.4f ms (total: %.2f sec)" % (tim.interval * 1000 / float(rep), tim.interval)

	with Timer() as tim:
		t.multi_is_in(X)

	print "Time per check: %.4f ms (total: %.2f sec)" % (tim.interval * 1000 / float(rep), tim.interval)

if __name__ == "__main__":
	import sys

	if "-P" in sys.argv:
		pr = cProfile.Profile()
		pr.enable()

	main()

	if "-P" in sys.argv:
		pr.disable()

		s = StringIO.StringIO()
		sortby = 'tottime'
		ps = pstats.Stats(pr, stream=s)
		ps.strip_dirs()
		ps.sort_stats(sortby)
		ps.print_stats()
		print s.getvalue()