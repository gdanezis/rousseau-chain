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

from consensus import Node, packageTx
import redis


class Listener(Thread):
    def __init__(self, redis_node, channels):
        Thread.__init__(self)
        self.node = redis_node
        self.redis = redis_node.r
        self.daemon = True

        self.pubsub = self.redis.pubsub()
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
        print "Tear down PubSub"
        self.pubsub.unsubscribe()


class RedisNode(Node):

    def __init__(self, start = [], quorum=1, name = None, 
            shard=None, shard_map=None,
            host='localhost', port=6379, db=0):
        self.r = redis.StrictRedis(host, port, db)
        self.shard_id = shard
        self.shard_map = shard_map

        if shard is not None:
            shard = shard_map[shard]

        Node.__init__(self, start, quorum, name, shard)

        if shard_map is None:
            # Register on the PubSub system
            self.client = Listener(self, ['votes'])
            self.client.start()
        else:
            self.client = Listener(self, ['votes:%s' % self.shard_id])
            self.client.start()

        self.RLogger = logging.getLogger()

    def __del__(self):
        self.client.teardown()


    def send(self, tx, msg):
        if self.shard_map is not None:
            for i in self.shard_map:
                (b0, b1) = self.shard_map[i]
                if b0<= tx[0] < b1:
                    self.r.publish('votes:%s' % i , msg)
        else:
            self.r.publish('votes' , msg)

    def receive(self, message):
        """ How to process incoming messages. """

        try:
            message = loads(message)
        except Exception as e:
            return

        # Ignore messages we sent
        if self.name == message["from"]:
            return

        tx = message['Tx']
        idx, deps, new_obj, data = tx
        
        if not idx == packageTx(data, deps, len(new_obj))[0]:
            raise Exception("Invalid transaction.")

        if not self._within_TX(tx):
            raise Exception("Transaction not of interest.")

        if message['action'] == "vote":

            n, l, v = tuple(message['vote'])
            vote = (n, tuple(l), v)

            self.RLogger.info("Receive vote (%s) for %s (%s)" % (v, idx[:6], self.name))
            
            if vote not in self.pending_vote[idx]:
                self.pending_vote[idx].add( vote )
                self.process(tx)
    
        if message['action'] == "commit":
            yesno = message['yesno']
            self.RLogger.info("Receive commit (%s) for %s (%s)" % (yesno ,idx[:6], self.name))

            if message["yesno"] == False:
                self.commit_no.add(idx)
            else:
                self.do_commit_yes(tx)

        if message['action'] == "process":
            self.process(tx)


    def on_vote(self, full_tx, vote):
        msg = dumps({ "action":"vote", "from":self.name, "Tx":full_tx, "vote":vote })
        
        idx, deps, new_obj, data = loads(msg)["Tx"]
        assert full_tx[0] == packageTx(data, deps, len(new_obj))[0]
        self.send(full_tx, msg)


    def on_commit(self, full_tx, yesno):
        msg = dumps({ "action":"commit", "from":self.name, "Tx":full_tx, "yesno":yesno })
        self.send(full_tx, msg)


