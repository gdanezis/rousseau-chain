import json
import time
import sys, traceback
import logging

from os import urandom
from binascii import hexlify
from collections import defaultdict, Counter

from hashlib import sha256
from struct import pack
from json import loads, dumps
from threading import Thread

from consensus import Node, packageTx, within_TX
import redis


class Listener(Thread):
    """ Helper function that monitors a channel and fires receive events. """
    def __init__(self, redis_node, channels):
        Thread.__init__(self)
        self.node = redis_node
        self.daemon = True

        self.pubsub = self.node.r.pubsub()
        self.pubsub.subscribe(channels)
    
    def work(self, item):
        try:
            self.node.receive(item['data'])
        except Exception as e:
            self.node.RLogger.info("Message Error: %s" % str(e))
            
    
    def run(self):
        for item in self.pubsub.listen():
            if item['type'] == 'message':
                self.work(item)

    def teardown(self):
        self.node.RLogger.info("Tear down PubSub (%s)" % self.node.name)
        self.pubsub.unsubscribe()


class RedisNode(Node):
    """ A consensus node based on Redis Pub/Sup. """

    def __init__(self, start = [], quorum=1, name = None, 
            shard=None, shard_map=None,
            host='localhost', port=6379, db=0):
        self.r = redis.StrictRedis(host, port, db)
        self.shard_id = shard
        self.shard_map = shard_map

        # Configure the shard
        if shard is not None:
            assert shard in shard_map
            shard = shard_map[shard]
            self.client = Listener(self, ['votes:%s' % self.shard_id])
        else:
            self.client = Listener(self, ['votes'])

        # Initialize the node for consensus.
        Node.__init__(self, start, quorum, name, shard)

        # Start the subscription loop and log.        
        self.client.start()
        self.RLogger = logging.getLogger()

    def __del__(self):
        self.client.teardown()

    def send(self, tx, msg):
        if self.shard_map is not None:
            for i in self.shard_map:
                (b0, b1) = self.shard_map[i]
                if within_TX(tx, b0, b1):
                    self.r.publish('votes:%s' % i , msg)
        else:
            self.r.publish('votes' , msg)

    def receive(self, message):
        """ How to process incoming messages. """

        try:
            # Ensure the messae decodes
            message = loads(message)

            # Make sure some basic stuctures are here
            originator = message["from"]
            tx, action = message['Tx'], message['action']
            idx, deps, new_obj, data = tx

        except Exception as e:
            raise Exception("Badly formatted messages: %s" % str(e))

        # Ignore messages we sent
        if self.name == originator:
            return
        
        if not idx == packageTx(data, deps, len(new_obj))[0]:
            raise Exception("Invalid transaction.")

        if not self._within_TX(tx):
            if action == "process":
                # We are going to re-route the message on a correct channel
                msg = dumps({ "action":"process", "from":self.name, "Tx":tx })
                self.send(tx, msg)
                return
            else:
                raise Exception("Transaction not of interest.")

        if action == "vote":
            # We process an incoming vote.
            n, l, v = message['vote']
            vote = (n, tuple(l), v)
            self.RLogger.info("Receive vote (%s) for %s (%s)" % (v, idx[:6], self.name))
            
            if vote not in self.pending_vote[idx]:
                self.pending_vote[idx].add( vote )
                self.process(tx)
    
        if action == "commit":
            # We process an incoming commit.
            yesno = message['yesno']
            self.RLogger.info("Receive commit (%s) for %s (%s)" % (yesno ,idx[:6], self.name))

            if yesno:
                self.do_commit_yes(tx)
            else:
                self.commit_no.add(idx)

        if action == "process":
            # We process a request
            self.process(tx)


    def on_vote(self, full_tx, vote):
        msg = dumps({ "action":"vote", "from":self.name, "Tx":full_tx, "vote":vote })
        self.send(full_tx, msg)


    def on_commit(self, full_tx, yesno):
        msg = dumps({ "action":"commit", "from":self.name, "Tx":full_tx, "yesno":yesno })
        self.send(full_tx, msg)


