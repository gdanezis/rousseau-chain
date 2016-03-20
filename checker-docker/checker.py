import sys
import time


import threading

def worker():
	t = time.time()
	stay = True
	while(stay):
		if time.time() - t > 2.0:
			print "Tick %s" % t
			t = time.time()


t = threading.Thread(target=worker)
t.daemon = True
t.start()

stay = True
while(stay):
	line = sys.stdin.readline().strip()

	if line == "":
		continue
	if line.lower() == "exit":
		stay = False
		continue


	print "Pong: %s" % line.lower()
