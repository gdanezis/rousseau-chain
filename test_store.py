from os import remove

from store import diskHashList

def test_direct_operations():
    try:
        remove("test_store.dat")
    except:
        pass

    s = diskHashList("test_store.dat")
    assert len(s) == 0

    s.append(b"A" * 32)
    s.append(b"B" * 32)
    s.append(b"C" * 32)

    assert len(s) == 3
    assert s[0] == b"A" * 32
    assert s[1] == b"B" * 32
    assert s[2] == b"C" * 32

    remove("test_store.dat")

def test_persistance():
    try:
        remove("test_store.dat")
    except:
        pass

    s = diskHashList("test_store.dat")
    assert len(s) == 0

    s.append(b"A" * 32)
    s.append(b"B" * 32)
    s.append(b"C" * 32)
    assert len(s) == 3

    del s

    s2 = diskHashList("test_store.dat")
    assert len(s2) == 3

    s2.append(b"A" * 32)
    s2.append(b"B" * 32)
    s2.append(b"C" * 32)
    assert len(s2) == 6
