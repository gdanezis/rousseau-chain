import cProfile
from hippiehug import Tree
import StringIO
import pstats

def main():
	t = Tree()	

	from os import urandom
	for _ in range(1000):
		item = urandom(32)
		t.add(item)
		assert t.is_in(item)
		assert not t.is_in(urandom(32))

if __name__ == "__main__":
	pr = cProfile.Profile()
	pr.enable()
	main()
	pr.disable()

	s = StringIO.StringIO()
	sortby = 'tottime'
	ps = pstats.Stats(pr, stream=s)
	ps.sort_stats(sortby)
	ps.strip_dirs()
	ps.print_stats()
	print s.getvalue()