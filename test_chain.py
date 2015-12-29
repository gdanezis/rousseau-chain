from chain import chain, check_evidence, pointFingers, initialH

# The unit tests, because we are professionals
# run with 'py.test -sv .'
def test_create():
    c = chain()
    assert c.head() == initialH

    assert c.add("Hello") == 0

def test_check():
    c = chain()
    c.add("Hello")

    # Add a lot of those
    for _ in xrange(1000):
        c.add("Hello")

    # Produce and check evidence
    e = c.evidence(100)
    assert check_evidence(c.head(), 100, e)
    assert check_evidence(c.head(), 100, e, entry="Hello")

def test_check_zero():
    c = chain()
    c.add("Hello")

    # Add a lot of those
    for _ in xrange(1000):
        c.add("Hello")

    # Produce and check evidence
    e = c.evidence(0)
    assert check_evidence(c.head(), 0, e)


def test_check_negative():
    c = chain()
    c.add("Hello")

    # Add a lot of those
    for _ in xrange(1000):
        c.add("Hello")

    e = c.evidence(100)

    import pytest
    # Wrong entry
    with pytest.raises(Exception):
        check_evidence(c.head(), 100, e, entry="Hello2")

    # Wrong nodes
    with pytest.raises(Exception):
        check_evidence(c.head(), 100, e, node="Hello2")

    # Wrong head
    with pytest.raises(Exception):
        check_evidence(c.nodes[-2], 100, e)

    # Wrong chain
    (en, no) = e
    no[88] = no[231]
    with pytest.raises(Exception):
        check_evidence(c.head(), 100, (en, no))

def test_ensure_prefix():
    c = chain()
    c.add("Hello")

    # Add a lot of those
    for _ in xrange(1000):
        c.add("Hello")

    seq = c.add("hello")
    head = c.head()

    for _ in xrange(1000):
        c.add("Hello")

    new_head = c.head()

    e = c.evidence(seq)
    assert check_evidence(new_head, seq, e, node=head)

def test_pointFingers():
    assert list(pointFingers(16)) == [15,14, 12, 8, 0]
    assert len(list(pointFingers(256))) == 9
