## This is the consensus protocol as implemented by the TLA+ / PlusCal code
#  in rousseau-chain/TLAproof/rousseau.tla

from collections import namedtuple, defaultdict, Counter

# Define the types of Messages, Process, Vote, and Commit

ProcessMsg = namedtuple("ProcessMsg", ["type", "tx"])
VoteMsg = namedtuple("VoteMsg", ["type", "tx", "shard", "node", "decision"])
CommitMsg = namedtuple("CommitMsg", ["type", "tx", "node", "decision"])

class AdvancedNode():
    def __init__(self, name, shard_map):
        self.name = name
        self.shard_name = shard_map

        # Stores 
        self.transactions = {}
        self.votes = defaultdict(set)

        self.accepted = set()
        self.rejected = set()
        self.resources = set()

        self.my_votes = set()


    def print_state(self):
        x = "=" * 20
        print "%s Name: %s %s" % (x, self.name, x)
        for t in sorted(self.votes):
            print t, list( (v.node, v.decision) for v in self.votes[t] )
        print "-" * (48 + len(self.name))
        print "Acc: ", self.accepted
        print "Rej: ", self.rejected
        print "-" * (48 + len(self.name))
        print "Obj:", self.resources
        print "Used:", self.used()
        print "-" * (48 + len(self.name))


    def namespace(self):
        return self.shard_name[self.name].namespace

    def already_voted_all(self, tx):
        for v in self.votes[tx.idx]:
            if self.name == v.node:
                return True
        return False

    def used(self):
        """ Defines which Object IDs are considered as used. """
        good_tx = {}
        used_obj = set()
        for tx_idx in self.accepted:
            used_obj |= self.transactions[tx_idx].deps

        still_valid_votes = { v.tx.idx for v in self.my_votes \
                    if v.decision and \
                        v.tx.deps & used_obj == set() and \
                        v.tx.idx not in self.rejected }

        for v_idx in still_valid_votes:
            used_obj |= { d for d in self.transactions[v_idx].deps if d[0] == self.namespace() }

        return used_obj


    def do_return(self, consume=True, log='', messages=[]):
        return (consume, log, messages)


    def do_all(self, msg):
        """ Selects the appropriate procedure to process and incoming message. """

        messages = [msg]
        to_send = set()
        to_repeat = set()
        all_logs = []
        while len(messages) > 0:
            m_i = messages.pop()

            if m_i.type == "Process":
                status, logs, out = self.do_process(m_i)
            elif m_i.type == "Vote":
                status, logs, out = self.do_vote(m_i)                
            elif m_i.type == "Commit":
                status, logs, out = self.do_commit(m_i)                
            else:
                assert False

            all_logs += [logs]
            to_send |= set(out)
            messages += list(out)
            if not status:
                to_repeat |= set([m_i])

        return to_send, to_repeat, all_logs


    def do_process(self, msg):
        """ Deal with a 'Process' message """

        if msg.type != "Process":
            return self.do_return(log="Incorrect type: %s" % msg.type)

        if msg.tx.idx not in self.transactions:
            self.transactions[msg.tx.idx] = msg.tx
        
        ## If it is not within our namespace no need to process
        if self.namespace() not in msg.tx.namespaces():
            return self.do_return(log="Not in NS")

        ## Check it is not already processed
        if msg.tx.idx in self.accepted or msg.tx.idx in self.rejected:
            return self.do_return(log="Already Committed: %s" % msg.tx.idx)

        ## Process only once
        if self.already_voted_all(msg.tx):
            return self.do_return(log="Already votes")

        # Which dependencies are local?
        local_deps = msg.tx.deps_by_namespace(self.namespace())

        if local_deps <= self.resources:
            decision = msg.tx.deps & self.used() == set()
                
            votes = set()
            for d in local_deps:
                votes.add(VoteMsg(type = "Vote",
                                  tx = msg.tx,
                                  shard = self.namespace(),
                                  node = self.name,
                                  decision = decision))

                if not decision:
                    print "="*20, self.name, "="*20
                    print "deps:", msg.tx.deps
                    print "used:",self.used()


            self.votes[msg.tx.idx] |= votes 
            self.my_votes |= votes
            return self.do_return(log="Voted", messages=votes)
        else:
            err = "Unknown Dependencies: %s" % (local_deps - self.resources)
            return self.do_return(consume=False, log=err) 
                                    

    def do_vote(self, msg):

        if msg.type != "Vote":
            return self.do_return(log="Incorrect type: %s" % msg.type)

        if msg.tx.idx not in self.transactions:
            self.transactions[msg.tx.idx] = msg.tx
        
        ## If it is not within our namespace no need to process
        if self.namespace() not in msg.tx.namespaces():
            return self.do_return(log="Not in NS")

        ## Check it is not already processed
        if msg.tx.idx in self.accepted or msg.tx.idx in self.rejected:
            return self.do_return(log="Already Committed: %s" % msg.tx.idx)

        # Do the tally
        all_namespaces = msg.tx.dep_namespaces()

        # Makes a list of all the sought votes / voters
        voters = {}
        for shard in self.shard_name.values():
            if shard.namespace in all_namespaces:
                voters[(shard.name, shard.namespace)] = None

        # Add the current vote if the voter was expected
        if (msg.node, msg.shard) in voters:
            self.votes[msg.tx.idx].add( msg )
        else:
            print "Error: transaction had invalid voter."

        # Now tally the votes
        for vote in self.votes[msg.tx.idx]:
            if (vote.node, vote.shard) in voters and voters[(vote.node, vote.shard)] == None:
                voters[(vote.node, vote.shard)] = vote.decision

        tally = defaultdict(Counter)
        total = defaultdict(int)
        for (node, shard), decision in voters.items():
            total[shard] += 1
            tally[shard].update([decision])

        shard_decisions = {}
        for ns in all_namespaces:
            if tally[ns][True] > total[ns] / 2:
                shard_decisions[ns] = True
            elif tally[ns][False] > total[ns] / 2:
                shard_decisions[ns] = False
            else:
                shard_decisions[ns] = None

        if False in shard_decisions.values():
            commit = CommitMsg(type = "Commit",
                                  tx = msg.tx,
                                  node = self.name,
                                  decision = False)

            return self.do_return(log="Rejected Tx: %s" % msg.tx.idx,
                                  messages=set([commit]))

        if None in shard_decisions.values():
            return self.do_return(log="No Decision Reached")

        if all(shard_decisions.values()):
            commit = CommitMsg(type = "Commit",
                                  tx = msg.tx,
                                  node = self.name,
                                  decision = True)

            return self.do_return(log="Accepted Tx: : %s" % msg.tx.idx,
                                  messages = set([commit]) )

    def do_commit(self, msg):

        if msg.type != "Commit":
            return self.do_return(log="Incorrect type: %s" % msg.type)

        if msg.tx.idx not in self.transactions:
            self.transactions[msg.tx.idx] = msg.tx
        
        ## If it is not within our namespace no need to process
        if self.namespace() not in msg.tx.namespaces():
            return self.do_return(log="Not in NS")

        ## Check it is not already processed
        if msg.tx.idx in self.accepted or msg.tx.idx in self.rejected:
            return self.do_return(log="Already Committed: %s" % msg.tx.idx)

        if msg.decision:
            self.resources |= { d for d in msg.tx.news if d[0] in self.namespace() } 
            self.accepted.add(msg.tx.idx)
            return self.do_return(log="Commit Yes: %s" % msg.tx.idx)
        else:
            self.rejected.add(msg.tx.idx)
            return self.do_return(log="Commit No: %s" % msg.tx.idx)
        

class Transaction:
    def __init__(self, idx, data, deps, news):
        self.idx = idx
        self.data = data
        self.deps = set(deps)
        self.news = set(news)

    def namespaces(self):
        return { namespace for namespace, objectID in self.deps | self.news }

    def dep_namespaces(self):
        return { namespace for namespace, objectID in self.deps }


    def deps_by_namespace(self, nsp):
        return { d for d in self.deps if d[0] == nsp }

    def __repr__(self):
        return "Tx(%s)" % self.idx


Shard = namedtuple("Shard", ["name", "namespace"])
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

    def print_votes(name, votes):
        print "%s:" % name
        for t in sorted(votes):
            print t, list( (v.node, v.decision) for v in votes[t] )

    nodeA.print_state()
    nodeB.print_state()
    nodeC.print_state()
    