# Make a hash chain with O(1) update and O(log(N)) proof of membership

from hashlib import sha256 as H
from struct import pack

# Some constants

# The initial value of any chain
# https://en.wikipedia.org/wiki/Om
initialH = H("Om").digest()

def pointFingers(seqLen):
    """ Returns the indexes for a particular sequence ID """
    seq = 1
    while seq <= seqLen:
        yield seqLen - seq
        seq = seq * 2

class chain(object):

    def __init__(self, entries=None, nodes=None):
        """ Create a new chain object """
        # This holds the actual log entries
        # it is a sequnence of byte arrays
        self.entries = []
        if entries is not None:
            self.entries = entries

        # The list of 'nodes' holding hashes of the current entry,
        # and a sequence of previous node hashes.
        self.nodes = []
        if nodes is not None:
            self.nodes = nodes


    def head(self):
        """ Return the head of the chain """
        if self.nodes == []:
            return initialH
        else:
            return self.nodes[-1]

    def add(self, entry):
        """ Add an entry at the end of the chain. Returns the index of the new entry. """

        # Create the new node head:
        entryH = H(entry).digest()

        nodeDigest = H(pack("L", len(self.entries)))
        nodeDigest.update(entryH)

        # Gather which other nodes are to be included:
        for i in pointFingers(len(self.entries)):
            nodeDigest.update(self.nodes[i])

        nodeH = nodeDigest.digest()

        self.entries.append(entryH)
        self.nodes.append(nodeH)

        return len(self.entries) - 1

    def evidence(self, seq):
        """ Gather evidence that the entry is at a sequence number in the chain. """
        entries = {}
        nodes = {}

        # Add evidence as required
        target = len(self.entries) - 1
        while seq not in entries:

            # Store the entry for the current target
            entries[target] = self.entries[target]
            nodes[target] = self.nodes[target]

            # Store the nodes on which we depend
            for i in pointFingers(target):
                nodes[i] = self.nodes[i]
                if i >= seq:
                    target = i

        # Return all necessary entries and nodes
        return entries, nodes


def check_evidence(head, seq, evidence, entry=None, node=None):
    """ Check that a bundle of evidence is correct, and correspond to,
    a known head, and optionally a known entry and known node. Returns
    True or raises an exception. """
    entries, nodes = evidence
    head_index = max(entries.keys())

    # CHECK 1: the head equals the head
    if not (head == nodes[head_index]):
        raise Exception("Wrong Head")

    # CHECK 2: all the hashes match
    target = head_index
    while target != seq:

        new_target = target

        # Make the digest
        d = H(pack("L", target))
        d.update(entries[target])
        for i in pointFingers(target):
            d.update(nodes[i])
            if i >= seq:
                new_target = i

        if d.digest() != nodes[target]:
            raise Exception("Broken Chain")

        target = new_target

    # CHECK 3: is the node correct?
    if node:
        if not (node == nodes[seq]):
            raise Exception("Wrong end node")

    # CHECK 4: is the actual entry correct?
    if entry:
        if not (H(entry).digest() == entries[seq]):
            raise Exception("Wrong end entry")

    return True
