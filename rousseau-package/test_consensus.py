import json
import time
from json import dumps
import logging
from threading import Timer as xTimer
from os import urandom
from random import sample, shuffle
from binascii import hexlify

from RedisConsensus import RedisNode
from MockConsensus import MockNode
from consensus import Node, packageTx, within_TX, make_shard_map

def test_random():
    resources = [hexlify(urandom(16)) for _ in range(300)]
    transactions = [(hexlify(urandom(16)), sample(resources,2), [], "") for _ in range(300)]

    n = Node(resources, 2)
    shuffle(transactions)
    tx_list = sample(transactions, 100)
    for tx in transactions:
        n.process(tx)

    n2 = Node(resources,2)
    n.gossip_towards(n2)
    for tx in transactions:
        n2.process(tx)

class Timer:    
    def __enter__(self):
        self.start = time.clock()
        return self

    def __exit__(self, *args):
        self.end = time.clock()
        self.interval = self.end - self.start

def test_wellformed():
    resources = [hexlify(urandom(16)) for _ in range(1000)]
    # def packageTx(data, deps, num_out)
    transactions = []
    for x in range(100):
        deps = sample(resources,2)
        data = json.dumps({"ID":x})
        tx = packageTx(data, deps, 2)
        transactions.append((tx, data))
    # [(hexlify(urandom(16)), sample(resources,2), []) for x in range(300)]

    n = Node(resources, 1)
    n.quiet = True
    shuffle(transactions)
    # tx_list = sample(transactions, 100)

    with Timer() as t:
        for tx, data in transactions:
            idx, deps, out, txdata = tx

            ## First perform the Tx checks
            assert packageTx(data, deps, 2) == tx

            ## Now process this transaction
            n.process(tx)
            
    print "Time taken: %2.2f sec" % (t.interval) 

def test_small():
    T1 = ("T1", ["A", "B"], [], "")
    T2 = ("T2", ["B", "C"], [], "")

    n = Node(["A", "B", "C"],1)
    n.process(T1)
    n.process(T2)
    assert "T1" in n.commit_yes
    assert "T2" not in n.commit_yes

def test_small_chain():
    T1 = ("T1", ["A"], ["B"], "")
    T2 = ("T2", ["B"], ["C"], "")

    n = Node(["A"],1)
    n.process(T1)
    n.process(T2)
    assert "C" in n.pending_available


def test_chain_conflict():
    T1 = ("T1", ["A"], ["B"], "")
    T2 = ("T2", ["A"], ["C"], "")
    T3 = ("T3", ["B"], ["D"], "")
    T4 = ("T4", ["C"], ["F"], "")

    n = Node(["A"],1)
    for tx in [T1, T2, T3, T4]:
        n.process(tx)


def test_quorum_simple():
    T1 = ("T1", ["A", "B"], [], "")
    T2 = ("T2", ["B", "C"], [], "")

    n1 = Node(["A", "B", "C"], 2)
    n2 = Node(["A", "B", "C"], 2)
    n3 = Node(["A", "B", "C"], 2)

    n1.process(T1)
    n2.process(T2)
    n2.process(T1)
    n3.process(T1)

    n1.gossip_towards(n2)
    n3.gossip_towards(n2)

    n2.process(T1)
    assert "T1" in n2.commit_yes


def test_quorum_threesome():
    T1 = ("T1", ["A", "B"], [], "")
    T2 = ("T2", ["B", "C"], [], "")
    T3 = ("T3", ["A", "C"], [], "")

    n1 = Node(["A", "B", "C"], 2)
    n2 = Node(["A", "B", "C"], 2)
    n3 = Node(["A", "B", "C"], 2)

    n1.process(T1)
    n2.process(T2)
    n3.process(T3)

    n1.process(T2)
    n1.process(T3)
    n2.process(T1)
    n2.process(T3)
    n3.process(T1)
    n3.process(T2)

    n1.gossip_towards(n3)
    n2.gossip_towards(n3)

    n3.process(T1)
    n3.process(T2)  
    n3.process(T3)
    assert "T1" in n3.commit_no
    assert "T2" in n3.commit_no
    assert "T3" in n3.commit_no



def test_shard_simple():
    T1 = ("333", ["444", "ccc"], [], "")
    T2 = ("bbb", ["444", "ddd"], [], "")

    n1 = Node(["444"], 1, name="n1", shard=["000", "aaa"])
    n2 = Node(["ccc", "ddd"], 1, name="n2", shard=["aaa", "fff"])

    n1.process(T1)
    n1.process(T2)
    print n1.pending_vote
    n2.process(T2)
    n2.process(T1)

    n1.gossip_towards(n2)

    n2.process(T1)
    n2.process(T2)
    
    assert '333' in n2.commit_yes

def test_shard_many():
    limits = sorted([hexlify(urandom(32)) for _ in range(100)])
    limits = ["0" * 64] + limits + ["f" * 64]

    pre = ["444", "ccc", "ddd"]
    nodes = [Node(pre, 1, name="n%s" % i, shard=[b0,b1]) for i, (b0, b1) in enumerate(zip(limits[:-1],limits[1:]))]

    T1 = ("333", ["444", "ccc"], [], "")
    T2 = ("bbb", ["444", "ddd"], [], "")

    n1 = [n for n in nodes if n._within_TX(T1)]
    n2 = [n for n in nodes if n._within_TX(T2)]

    assert len(n1) == 3 and len(n2) == 3

    for n in n1:
        n.process(T1)

    for n in n2:
        n.process(T2)       


def test_mock_shard_many():
    limits = sorted([hexlify(urandom(32)) for _ in range(100)])
    limits = ["0" * 64] + limits + ["f" * 64]

    _, _, [A, B, C], txdata = packageTx(data="data1", deps=[], num_out=3)

    pre = [A, B, C]
    nodes = [MockNode(pre, 1, name="n%s" % i, shard=[b0,b1]) for i, (b0, b1) in enumerate(zip(limits[:-1],limits[1:]))]

    def send(msg):
        # print "Send: " + str(msg)
        tx = msg["Tx"]
        ns = [n for n in nodes if n._within_TX(tx)]
        for n in ns:
            n.receive(msg)

    for n in nodes:
        n.set_send(send)

    T1 = packageTx("333", [A, B], 2)
    T2 = packageTx("bbb", [A, C], 2)

    n1 = [n for n in nodes if n._within_TX(T1)]
    n2 = [n for n in nodes if n._within_TX(T2)]

    # assert len(n1) == 3 and len(n2) == 3

    for n in n1:
        n.process(T1)

    for n in n2:
        n.process(T2)       




def test_redis_consensus():
    # logging.getLogger().setLevel(logging.DEBUG)

    _, _, [A, B, C], txdata = packageTx(data="data1", deps=[], num_out=3)

    pre = [A, B, C]
    node1 = RedisNode(pre, 2, name="n0")
    node2 = RedisNode(pre, 2, name="n1")
    node3 = RedisNode(pre, 2, name="n2")

    T1 = packageTx("333", [A, B], 2)
    T2 = packageTx("bbb", [A, C], 2)
    
    node1.process(T1)
    node1.process(T2)


def test_distribution():

    shard_map = make_shard_map(100)

    from collections import defaultdict
    d = defaultdict(int)

    for x in range(1000):
        T1 = packageTx("333%s" % x, [], 2)
        for i in shard_map: 
            b0, b1 = shard_map[i]
            if within_TX(T1, b0, b1):
                d[i] += 1
    
    for i in sorted(d):
        print "%3d | %s" % (i, "=" * d[i])

def test_redis_shard_many():
    shard_map = make_shard_map(100)
    
    _, _, [A, B, C], txdata = packageTx(data="data1", deps=[], num_out=3)

    pre = [A, B, C]
    nodes = [RedisNode(pre, 1, name="n%s" % i, shard=i, shard_map=shard_map) for i in shard_map]

    T1 = packageTx("333", [A, B], 2)
    T2 = packageTx("bbb", [A, C], 2)

    # Relevent Nodes
    r1nodes = [n for n in nodes if n._within_TX(T1)]
    r1nodes[0].process(T1)

    # Relevent Nodes
    r2nodes = [n for n in nodes if n._within_TX(T2)]
    r2nodes[0].process(T2)

    def test_condition():
        Good = True
        Good &= T1[0] in r1nodes[-1].commit_yes
        Good &= T1[0] in r1nodes[0].commit_yes
        Good &= T2[0] in r2nodes[-1].commit_no
        Good &= T2[0] in r2nodes[0].commit_no
        assert Good
        print "All: %s" % Good

    t = xTimer(3.0, test_condition)
    t.start()


def test_redis_shard_reflect():

    logging.getLogger().setLevel(logging.DEBUG)
    shard_map = make_shard_map(100)
    
    _, _, [A, B, C], txdata = packageTx(data="data1", deps=[], num_out=3)

    pre = [A, B, C]
    nodes = [RedisNode(pre, 1, name="n%s" % i, shard=i, shard_map=shard_map) for i in shard_map]

    T1 = packageTx("333", [A, B], 2)
    T2 = packageTx("bbb", [A, C], 2)
    T1_json = dumps({ "action":"process", "from":"ext", "Tx":T1 })
    T2_json = dumps({ "action":"process", "from":"ext", "Tx":T2 })

    nodes[0].r.publish('votes:%s' % i , T1_json)
    nodes[0].r.publish('votes:%s' % i , T2_json)

    # Relevent Nodes
    r1nodes = [n for n in nodes if n._within_TX(T1)]
    r2nodes = [n for n in nodes if n._within_TX(T2)]
    

    def test_condition():
        Good = True
        Good &= T1[0] in r1nodes[-1].commit_yes
        Good &= T1[0] in r1nodes[0].commit_yes
        Good &= T2[0] in r2nodes[-1].commit_no
        Good &= T2[0] in r2nodes[0].commit_no
        assert Good
        print "All: %s" % Good

    t = xTimer(3.0, test_condition)
    t.start()
