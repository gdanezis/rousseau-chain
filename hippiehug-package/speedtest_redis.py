import cProfile
from hippiehug import Tree, RedisStore
import StringIO
import pstats
import redis


def _flushDB():
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    r.flushdb()


def main():
	r = RedisStore()
	t = Tree(store=r)	

	from os import urandom
	for _ in range(1000):
		item = urandom(32)
		t.add(item)
		assert t.is_in(item)
		assert not t.is_in(urandom(32))

if __name__ == "__main__":
	pr = cProfile.Profile()
	_flushDB()

	pr.enable()
	main()
	pr.disable()

	s = StringIO.StringIO()
	sortby = 'tottime'
	ps = pstats.Stats(pr, stream=s)
	ps.strip_dirs()
	ps.sort_stats(sortby)
	ps.print_stats()
	print s.getvalue()