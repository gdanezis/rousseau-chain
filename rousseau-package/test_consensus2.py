from consensus2 import AdvancedNode, Shard, Transaction, ProcessMsg

def test_process():
    shards = {
        "A": Shard("A","a", ),
        "B": Shard("B","b", ),
        "C": Shard("C","b", ),
        "D": Shard("D","b", )
    }

    nodeA = AdvancedNode("A", shards)
    assert nodeA.namespace() == "a"
    nodeB = AdvancedNode("B", shards)
    nodeC = AdvancedNode("C", shards)
    nodeC = AdvancedNode("D", shards)

    nodeA.resources |= set([("a", "ID1")])
    # T0 = Transaction("T0", "T0 Data", [], [("a", "ID2")])
    T1 = Transaction("T1", "T1 Data", [("a", "ID1")], [("a", "ID2")])
    T2 = Transaction("T2", "T2 Data", [("a", "ID1")], [("a", "ID3")])
    res, reason, m1 = nodeA.do_process(ProcessMsg("Process",T1))
    res, reason, m2 = nodeA.do_process(ProcessMsg("Process",T2))
    print reason

    queue = list(set(m1) | set(m2))
    queue2 = set()
    for q in queue:
        _,_, m3 = nodeA.do_vote(q)
        queue2 |= set(m3)

    for q in queue2:
        print nodeA.do_commit(q)

    assert ("a", "ID2") in nodeA.resources
    assert ("a", "ID3") not in nodeA.resources


def run(trans, runnable):
    from random import choice
    steps = 0
    results = set()
    active = set()
    for n, _ in runnable:
        active |= n.resources

    xbuffer = []
    while steps < 1000:
        new_trans = set()
        for T in trans:
            if T.deps <= active and ProcessMsg("Process", T) not in xbuffer:
                xbuffer += [ProcessMsg("Process", T)]

        steps += 1
        msg = choice(xbuffer)
        node, proc = choice(runnable)

        node.out = []
        to_send, to_repeat, all_logs = proc(msg)

        for r in all_logs:
            if r not in results:
                print steps, r
                results.add(r)


        for msg in to_send:
            if msg.type == "Commit" and msg.decision:
                active |= msg.tx.news

        xbuffer += to_send


def test_many():
    shards = {
        "A": Shard("A","a", ),
        "B": Shard("B","b", ),
        "C": Shard("C","b", ),
        "D": Shard("D","b", )
    }

    nodeA = AdvancedNode("A", shards)
    assert nodeA.namespace() == "a"
    nodeB = AdvancedNode("B", shards)
    nodeC = AdvancedNode("C", shards)
    nodeC = AdvancedNode("D", shards)

    nodeA.resources |= set([("a", "ID1")])
    # T0 = Transaction("T0", "T0 Data", [], [("a", "ID2")])
    T1 = Transaction("T1", "T1 Data", [("a", "ID1")], [("a", "ID2")])
    T2 = Transaction("T2", "T2 Data", [("a", "ID1")], [("a", "ID3")])
    
    Td = T1.to_dict()
    Tp = Transaction.from_dict(Td)
    assert T1 == Tp
    assert not T2 == Tp

    xbuffer = [T1, T2]
    runnable = [(nodeA, nodeA.do_all)]

    run(xbuffer, runnable)

def test_many_shards():
    shards = {
        "A": Shard("A","a", ),
        "B": Shard("B","b", ),
        "C": Shard("C","b", ),
        "D": Shard("D","b", )
    }

    nodeA = AdvancedNode("A", shards)
    assert nodeA.namespace() == "a"
    nodeB = AdvancedNode("B", shards)
    nodeC = AdvancedNode("C", shards)
    nodeD = AdvancedNode("D", shards)

    nodeA.resources |= set([("a", "ID1")])
    nodeB.resources |= set([("b", "IDb")])
    nodeC.resources |= set([("b", "IDb")])
    nodeD.resources |= set([("b", "IDb")])

    # T0 = Transaction("T0", "T0 Data", [], [("a", "ID2")])
    T1 = Transaction("T1", "T1 Data", [("a", "ID1"), ("b", "IDb")], [("a", "ID2")])
    T2 = Transaction("T2", "T2 Data", [("b", "IDb")], [("b", "ID3")])
    
    xbuffer = [T1, T2]
    runnable = [(nodeA, nodeA.do_all), (nodeB, nodeB.do_all), (nodeC, nodeC.do_all), (nodeD, nodeD.do_all)]

    run(xbuffer, runnable)

def test_classic4():
    # import logging
    # logging.basicConfig(level=logging.DEBUG)

    shards = {
        "A": Shard("A","a", ),
        "B": Shard("B","b", ),
        "C": Shard("C","c", ),
    }

    nodeA = AdvancedNode("A", shards)
    nodeB = AdvancedNode("B", shards)
    nodeC = AdvancedNode("C", shards)

    nodeA.resources |= set([("a", "IDa")])
    nodeB.resources |= set([("b", "IDb")])
    nodeC.resources |= set([("c", "IDc")])

    # T0 = Transaction("T0", "T0 Data", [], [("a", "ID2")])
    T1 = Transaction("T1", "T1 Data", [("a", "IDa"), ("b", "IDb")], [("a", "IDa2")])
    T2 = Transaction("T2", "T2 Data", [("b", "IDb"), ("c", "IDc")], [("b", "IDb2")])
    T3 = Transaction("T3", "T3 Data", [("a", "IDa"), ("b", "IDb2")], [("c", "IDc3")])
    T4 = Transaction("T4", "T4 Data", [("c", "IDc"), ("a", "IDa2")], [("c", "IDc4")])

    xbuffer = [T1, T2, T3, T4]
    runnable = [(nodeA, nodeA.do_all), (nodeB, nodeB.do_all), (nodeC, nodeC.do_all)]

    run(xbuffer, runnable)

    nodeA.print_state()
    nodeB.print_state()
    nodeC.print_state()
