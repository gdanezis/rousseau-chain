## This is the consensus protocol as implemented by the TLA+ / PlusCal code
#  in rousseau-chain/TLAproof/rousseau.tla

from collections import namedtuple, defaultdict, Counter
from logging import getLogger

# Define the types of Messages, Process, Vote, and Commit
ProcessMsg = namedtuple("ProcessMsg", ["type", "tx"])
VoteMsg = namedtuple("VoteMsg", ["type", "tx", "shard", "node", "decision"])
CommitMsg = namedtuple("CommitMsg", ["type", "tx", "node", "decision"])

# Define what makes a shard
Shard = namedtuple("Shard", ["name", "namespace"])

class AdvancedNode(object):
    def __init__(self, name, shard_map):
        """ Initialize with a name and a map of the shards, by name. """

        # Some basic checks on the shards
        assert name in shard_map
        assert type(shard_map) == dict
        assert all(type(v) == Shard for v in shard_map.values())

        # Local Parameters
        self.name = name
        self.shard_name = shard_map
        self.logger = getLogger()

        # Stores 
        self.transactions = {}

        # These, we need to remember
        self.my_votes = set()

        # Other Storess
        self.votes = defaultdict(set)
        self.accepted = set()
        self.rejected = set()
        self.resources = set()

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
        """ Returns the the namespace managed by this node. """
        return self.shard_name[self.name].namespace

    def already_voted_all(self, tx):
        """ Tests whether we have cast a vote for a Transaction ID. """
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
        """ Mediates the results of processing a message."""
        self.logger.info("%s: %s" % (self.name, log))
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
        """ Deal with a 'Process' message. """

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
        """ Deal with a 'Vote' message. """

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

        # Compile the number of voters and votes per shard
        tally = defaultdict(Counter)
        total = defaultdict(int)
        for (node, shard), decision in voters.items():
            total[shard] += 1
            tally[shard].update([decision])

        # Computes the decision for each shard.
        shard_decisions = {}
        for ns in all_namespaces:
            if tally[ns][True] > total[ns] / 2:
                shard_decisions[ns] = True
            elif tally[ns][False] > total[ns] / 2:
                shard_decisions[ns] = False
            else:
                shard_decisions[ns] = None

        # Even a single confirmed False kills this transaction.
        if False in shard_decisions.values():
            commit = CommitMsg(type = "Commit",
                                  tx = msg.tx,
                                  node = self.name,
                                  decision = False)

            return self.do_return(log="Rejected Tx: %s" % msg.tx.idx,
                                  messages=set([commit]))

        # If some input is unknonw wait on this transaction
        if None in shard_decisions.values():
            return self.do_return(log="No Decision Reached")

        # If all are True then accept this Transaction.
        if all(shard_decisions.values()):
            commit = CommitMsg(type = "Commit",
                                  tx = msg.tx,
                                  node = self.name,
                                  decision = True)

            return self.do_return(log="Accepted Tx: : %s" % msg.tx.idx,
                                  messages = set([commit]) )

    def do_commit(self, msg):
        """ Deal with a 'Commit' message """

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
        

class Transaction(object):
    def __init__(self, idx, data, deps, news):
        """ Initialize the transactions """
        self.idx = idx
        self.data = data
        self.deps = set(deps)
        self.news = set(news)

    def namespaces(self):
        """ Extract the namespaces concerned with this Transaction. """
        return { namespace for namespace, objectID in self.deps | self.news }

    def dep_namespaces(self):
        """ Extract the namespaces related to the dependencies. """
        return { namespace for namespace, objectID in self.deps }

    def deps_by_namespace(self, nsp):
        """ Return the dependencies related to a namespace. """
        return { d for d in self.deps if d[0] == nsp }

    def __repr__(self):
        """ A string representation of the Transaction. """
        return "Tx(%s)" % self.idx

    def __eq__(self, other):
        """ Tests for equality. """
        return self.idx == other.idx and \
               self.data == other.data and \
               self.deps == other.deps and \
               self.news == other.news

    def to_dict(self):
        return {"idx": self.idx, 
                "data": self.data,
                "deps": list(self.deps),
                "news": list(self.news)}
    
    @staticmethod
    def from_dict(d):
        return Transaction(d["idx"], d["data"], set(d["deps"]), set(d["news"]))
