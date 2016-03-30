# This is a convergence simulation for gossip based consensus.

import json
import time
import logging

from os import urandom
from random import sample, shuffle
from binascii import hexlify
from collections import defaultdict, Counter

from hashlib import sha256
from struct import pack

def make_shard_map(num = 100):
    """ Makes a map for 'num' shards (defaults to 100). """

    limits = []
    MAX = 2**16

    for l in range(0, MAX - 1, MAX / num):
        l_lower = hexlify(pack(">H", l)) + ("00" * 20)
        limits.append(l_lower)

    limits = limits + ["f" * 64]

    shard_map = []
    for i, (b0, b1) in enumerate(zip(limits[:-1],limits[1:])):
        shard_map.append((i, (b0, b1)))
    shard_map = dict(shard_map)

    return shard_map


def within_ID(idx, b0, b1):
    """ Tests whether an object identifer is within the 
    remit of the shard bounds. """
    return b0 <= idx < b1


def within_TX(Tx, b0, b1):
    """ Test whether the transaction and its dependencies are
    within the shard bounds. """
    
    idx, deps, outs, txdata = Tx
    if within_ID(idx, b0, b1):
        return True

    if any(within_ID(d, b0, b1) for d in deps):
        return True

    if any(within_ID(d, b0, b1) for d in outs):
        return True

    return False


def h(data):
    """ Define the hash function used in the system. This is used to
    derive transaction and object identifiers. """
    return hexlify(sha256(data).digest()[:20])


def packageTx(data, deps, num_out):
    """ Package some transaction data into an appropriate identifier,
    and resulting new object identifiers. """
    hx = sha256(data)
    for d in sorted(deps):
        hx.update(d)

    actualID = hx.digest()
    actualID = actualID[:-2] + pack("H", 0)

    out = []
    for i in range(num_out):
        out.append(actualID[:-2] + pack("H", i+1))

    return (hexlify(actualID), sorted(deps), map(hexlify,out), data)

class Node:
    """ A class representing an authority participating in the consensus. """

    def __init__(self, start = [], quorum=1, name = None, shard=None):
        self.transactions = {}

        self.quorum = quorum
        self.name = name if name is not None else urandom(16)
        self.pending_vote = defaultdict(set)

        if shard is None:
            self.shard = ["0"*64, "f"*64]
        else:
            self.shard = shard

        self.pending_available = set(o for o in start if self._within_ID(o))
        self.pending_used = set()

        self.commit_yes  = set()
        self.commit_no   = set()
        # self.commit_available = set(start)
        self.commit_used = set()

        self.quiet = False

        if __debug__:
            self.start = set(o for o in start if self._within_ID(o))
            self.cache = { }


    def _within_ID(self, idx):
        """ Tests whether an object identifer is within the 
        remit of this Node. """
        return within_ID(idx, self.shard[0], self.shard[1])
        

    def _within_TX(self, Tx):
        """ Test whether the transaction and its dependencies are
        within the remit of this Node. """
        ## Tests whether a transaction is related to this node in 
        ## any way. If not there is no case for processing it.
        return within_TX(Tx, self.shard[0], self.shard[1])


    def gossip_towards(self, other_node):
        """ A primitive way to probagate information. """
        for k, v in self.pending_vote.iteritems():
            other_node.pending_vote[k] |= v

        # Should we process votes again here?
        other_node.commit_yes |= self.commit_yes
        other_node.commit_no |= self.commit_no
        assert other_node.commit_yes & other_node.commit_no == set()

        # other_node.commit_available |= self.commit_available
        other_node.commit_used |= self.commit_used


    def on_vote(self, full_tx, vote):
        """ What the Node does when a transaction vote is cast. """
        pass


    def on_commit(self, full_tx, yesno):
        """ What to do when a transaction commit is cast. """
        pass


    def process(self, Tx):
        """ Process a transaction to vote or commit it. """

        if not self._within_TX(Tx):
            return

        # Cache the transaction
        self.transactions[Tx[0]] = Tx

        # Process the transaction
        logging.info("Process %s (%s)" % (Tx[0][:8], self.name))
            
        x = True
        while(x):
            x = self._process(Tx)


    def do_commit_yes(self, Tx):
        """ What to do when commiting a transaction to the positive log. """

        if __debug__:
            self.cache[Tx[0]] = Tx

        idx, deps, new_obj, txdata = Tx
        self.commit_yes.add(idx)
        self.pending_available |= set(o for o in new_obj if self._within_ID(o)) ## Add new transactions here
        self.commit_used |= set(o for o in deps if self._within_ID(o))


    def _check_invariant(self):
        """ An internal debugging function to ensure all invariants hold. """

        all_objects = set(self.start)
        used_objects = set()

        for txa in self.commit_yes:
            assert txa in self.cache
            idx, deps, new_obj, data = self.cache[txa]
            all_objects |= set(o for o in new_obj if self._within_ID(o))
            used_objects |= set(o for o in deps if self._within_ID(o))

        assert all_objects == self.pending_available
        assert used_objects == self.commit_used
        
        for o in self.commit_used:
            assert self._within_ID(o)

        assert used_objects <= all_objects

        potentially_used = { xd for xd, xtx in self.pending_used if xtx not in self.commit_no} 
        actually_available = self.pending_available - potentially_used
        assert (all_objects - used_objects) - potentially_used == actually_available

        return True


    def _process(self, Tx):

        if __debug__:
            self.cache[Tx[0]] = Tx
            self._check_invariant()

        if not self._within_TX(Tx):
            return False

        idx, deps, new_obj, txdata = Tx
        all_deps = set(deps)
        deps = {d for d in deps if self._within_ID(d)}
        new_obj = set(new_obj) # By construction no repeats & fresh names

        if (idx in self.commit_yes or idx in self.commit_no):
            # Do not process twice
            logging.info("Do nothing for %s (%s)" % (idx[:6], self.name))

            return False # No further progress can be made

        else:
            if deps & self.commit_used != set():
                
                # Some dependencies are used already!
                # So there is no way we will ever accept this
                # and neither will anyone else

                self.commit_no.add(idx)
                self.on_commit( Tx, False )

                logging.info("Commit no for %s (%s)" % (idx[:6], self.name))
                return False # there is no further work on this.

            # If we cannot exclude it out of hand then we kick in
            # the consensus protocol by considering it a candidate.

        xdeps = tuple(sorted(list(deps)))

        if not ( (self.name, xdeps, True) in self.pending_vote[idx] or (self.name, xdeps, False) in self.pending_vote[idx]):
            # We have not considered this as a pending candidate before
            # So now we have to vote on it.

            if deps.issubset(self.pending_available):
                # We have enough information on the transactions this
                # depends on, so we can vote.

                # Make a list of used transactions:
                used = { xd for xd, xtx in self.pending_used if xtx not in self.commit_no} 
                # and xd not in self.commit_used }
                ## CHECK CORRECTNESS: Do we update on things that are eventually used?

                if set(deps) & used == set() and set(deps) & self.commit_used == set():
                    # We cast a 'yes' vote -- since it seems that there
                    # are no conflicts for this transaction in our pending list.

                    self.pending_vote[idx].add( (self.name, xdeps, True) )
                    self.pending_used |= set((d, idx) for d in deps)
                    
                    self.on_vote( Tx, (self.name, xdeps, True) )

                    # TODO: add new transactions to available here
                    #       Hm, actually we should not until it is confirmed.
                    # self.pending_available |= new_obj ## Add new transactions here

                    logging.info("Pending yes for %s (%s)" % (idx[:6], self.name))
                    return True

                else:
                    # We cast a 'no' vote since there is a conflict in our
                    # history of transactions.
                    self.pending_vote[idx].add( (self.name, xdeps, False) )
                    self.on_vote( Tx, (self.name, xdeps, False) )

                    logging.info("Pending no for %s (%s)" % (idx[:6], self.name))
                    return True
            else:
                logging.info("Unknown prerequisites for %s (%s)" % (idx[:6], self.name))

                # We continue in case voting helps move things. This
                # happens in case others know about this transaction.

        if self.shard[0] <= idx < self.shard[1] or deps != set():
            # Only process the final votes if we are in charde of this
            # shard for the transaction or any dependencies.

            Votes = Counter()
            for oname, odeps, ovote in self.pending_vote[idx]:
                for d in odeps:
                    Votes.update( [(d, ovote)] )

            yes_vote = all( Votes[(d, True)] >= self.quorum for d in all_deps )
            no_vote = any( Votes[(d, False)] >= self.quorum for d in all_deps )

            ## Time to count votes for this transaction
            if yes_vote: # Counter(x for _,x in self.pending_vote[idx])[True] >= self.quorum:
                # We have a Quorum for including the transaction. So we update
                # all the committed state monotonically.
                self.do_commit_yes(Tx)

                self.on_commit( Tx, True )

                ## CHECK CORRECT: Should I add the used transactions to self.pending_used?
                logging.info("Commit yes for %s (%s)" % (idx[:6], self.name))
                return False

            if no_vote: #Counter(x for _,x in self.pending_vote[idx])[False] >= self.quorum:
                # So sad: there is a quorum for rejecting this transaction
                # so we will now add it to the 'no' bucket.
                # Optional TODO: invalidate in the pending lists 

                self.commit_no.add(idx)

                self.on_commit( Tx, False )
                logging.info("Commit no for %s (%s)" % (idx[:6], self.name))
                return False

        return False # No further work


