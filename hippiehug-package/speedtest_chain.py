import cProfile
from hippiehug import DocChain
import StringIO
import pstats
from os import urandom

N = 10000

def main(data):
	c = DocChain()

	for i in range(N):
		c.multi_add([data[i]])

if __name__ == "__main__":
	pr = cProfile.Profile()

	data = []
	for _ in range(N):
		data += [urandom(32)]


	pr.enable()
	main(data)
	pr.disable()

	s = StringIO.StringIO()
	sortby = 'tottime'
	ps = pstats.Stats(pr, stream=s)
	ps.sort_stats(sortby)
	ps.strip_dirs()
	ps.print_stats()
	print s.getvalue()