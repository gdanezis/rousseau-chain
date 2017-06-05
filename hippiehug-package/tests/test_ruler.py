## This is a silly test of a "ruler" based hash chain core.

def test_rule():
	Ruler_breaks = [8**i for i in range(32)]

	prev = [None] * len(Ruler_breaks)
	for j in range(100):

		for seq, (notch, old) in enumerate(zip(Ruler_breaks, prev)):
			if j == 0:
				prev = prev
				break

			if (j - 1) % notch == 0:
				prev[seq] = (j-1)


if __name__ == "__main__":
	test_rule()