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
        # self.exist_resources = set()
        self.resources = set()

        self.my_votes = set()

        ## ---
        self.out = []

    def print_state(self):
        print "=" * 40
        print "Name: %s" % self.name
        for t in sorted(self.votes):
            print t, list( (v.node, v.decision) for v in self.votes[t] )
        print "-" * 40
        print "Acc: ", self.accepted
        print "Rej: ", self.rejected
        print "-" * 40
        print "Obj:", self.resources
        print "Used:", self.used()



    def namespace(self):
        return self.shard_name[self.name].namespace

    def already_voted_all(self, tx):
        for v in self.votes[tx.idx]:
            if self.name == v.node:
                return True

        return False

    def used(self):
        # with(good_votes = {v 
        #    \in votes[self] : (\A iacc \in accepted[self]: 
        #      v # iacc => TTT[v].dep \intersect TTT[iacc].dep = {})}) 
        # with(used = UNION { TTT[f0].dep: 
        #       f0 \in (accepted[self] \cup ( good_votes \ rejected[self]))}){
        # if (TTT[m[2]].dep \intersect resources[self] # {}){

        good_tx = {}
        used_obj = set()
        for tx_idx in self.accepted:
            used_obj |= self.transactions[tx_idx].deps


        still_valid_votes = { v.tx.idx for v in self.my_votes \
                    if v.decision and \
                        v.tx.deps & used_obj == set() and \
                        v.tx.idx not in self.rejected }

        used_obj = set()
        for v_idx in self.accepted.union(still_valid_votes):
            used_obj |= self.transactions[v_idx].deps

        return used_obj


    def do_all(self, msg):
        if msg.type == "Process":
            return self.do_process(msg)
        elif msg.type == "Vote":
            return self.do_vote(msg)
        elif msg.type == "Commit":
            return self.do_commit(msg)
        else:
            return False, "Unknown type: %s" % (msg.type)

    def do_process(self, msg):

        if msg.type != "Process":
            return True, "Incorrect type: %s" % msg.type

        if msg.tx.idx not in self.transactions:
            self.transactions[msg.tx.idx] = msg.tx
        
        ## If it is not within our namespace no need to process
        if self.namespace() not in msg.tx.namespaces():
            return True, "Not in NS"

        ## Check it is not already processed
        if msg.tx.idx in self.accepted or msg.tx.idx in self.rejected:
            return True, "Already Committed: %s" % msg.tx.idx

        ## Process only once
        if self.already_voted_all(msg.tx):
            return True, "Already votes"

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

            self.votes[msg.tx.idx] |= votes 
            self.my_votes |= votes
            self.send(votes)
            return True, "Voted"
        else:
            return False, "Unknown Dependencies: %s" % (local_deps - self.resources) 
                                    
        return True, "End of func"

    def do_vote(self, msg):

        if msg.type != "Vote":
            return True, "Incorrect type: %s" % msg.type

        if msg.tx.idx not in self.transactions:
            self.transactions[msg.tx.idx] = msg.tx
        
        ## If it is not within our namespace no need to process
        if self.namespace() not in msg.tx.namespaces():
            return True, "Not in NS"

        ## Check it is not already processed
        if msg.tx.idx in self.accepted or msg.tx.idx in self.rejected:
            return True, "Already Committed: %s" % msg.tx.idx

        # namedtuple("VoteMsg", ["type", "tx", "shard", "node", "decision"])

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

        # namedtuple("CommitMsg", ["type", "tx", "node", "decision"])

        if False in shard_decisions.values():
            commit = CommitMsg(type = "Commit",
                                  tx = msg.tx,
                                  node = self.name,
                                  decision = False)

            self.send(set([commit]))

            return True, "Rejected Tx: %s" % msg.tx.idx

        if None in shard_decisions.values():
            return True, "No Decision Reached"

        if all(shard_decisions.values()):
            commit = CommitMsg(type = "Commit",
                                  tx = msg.tx,
                                  node = self.name,
                                  decision = True)

            self.send(set([commit]))
            return True, "Accepted Tx: : %s" % msg.tx.idx

    def do_commit(self, msg):

        if msg.type != "Commit":
            return True, "Incorrect type: %s" % msg.type

        if msg.tx.idx not in self.transactions:
            self.transactions[msg.tx.idx] = msg.tx
        
        ## If it is not within our namespace no need to process
        if self.namespace() not in msg.tx.namespaces():
            return True, "Not in NS"

        ## Check it is not already processed
        if msg.tx.idx in self.accepted or msg.tx.idx in self.rejected:
            return True, "Already Committed: %s" % msg.tx.idx

        if msg.decision:
            self.resources |= { d for d in msg.tx.news if d[0] in self.namespace() } 
            self.accepted.add(msg.tx.idx)
            return True, "Commit Yes: %s" % msg.tx.idx
        else:
            self.rejected.add(msg.tx.idx)
            return True, "Commit No: %s" % msg.tx.idx

    def send(self, msgs):
        self.out += list(msgs)
        

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
    res, reason = nodeA.do_process(ProcessMsg("Process",T1))
    res, reason = nodeA.do_process(ProcessMsg("Process",T2))
    print reason

    queue = list(nodeA.out)
    nodeA.out = []
    for q in queue:
        print nodeA.do_vote(q)

    queue = list(nodeA.out)
    nodeA.out = []
    for q in queue:
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
        _, r = proc(msg)
        if r not in results:
            print steps, r
            results.add(r)

        for msg in node.out:
            if msg.type == "Commit" and msg.decision:
                active |= msg.tx.news

        xbuffer += node.out


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
    
