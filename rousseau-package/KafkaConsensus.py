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

from consensus import Node, packageTx, within_TX, make_shard_map
# import redis

# from kafka import SimpleProducer, KafkaClient
# from kafka import KafkaConsumer
from kafka import KafkaClient, SimpleProducer, SimpleConsumer

logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    level=logging.WARNING
)

def Tx2json(Tx):
    fields = ["id", "depends_on", "creates", "contents"]
    return dict(zip(fields, Tx))

def json2Tx(jTx):
    fields = ["id", "depends_on", "creates", "contents"]
    return map(lambda k: jTx[k], fields)

class Listener(Thread):
    """ Helper function that monitors a channel and fires receive events. """
    def __init__(self, node, channel):
        Thread.__init__(self)
        self.node = node
        self.daemon = True

        self.consumer = SimpleConsumer(self.node.kafka, 'consensus', channel)
    
    def work(self, item):
        try:
            print "Process (%s): %s" % (self.node.name, item)
            self.node.receive(item)
        except Exception as e:
            self.node.RLogger.info("Message Error: %s" % str(e))
            
    
    def run(self):
        for message in self.consumer:
            self.work(message.message.value)

    def teardown(self):
        self.node.RLogger.info("Tear down PubSub (%s)" % self.node.name)
        

class KafkaNode(Node):
    """ A consensus node based on Redis Pub/Sup. """

    def __init__(self, start = [], quorum=1, name = None, 
            shard=None, shard_map=None,
            host='localhost', port=9092):
        self.client = None
        self.channel_name = None

        self.kafka = KafkaClient('%s:%s' % (host,port)) # redis.StrictRedis(host, port, db)
        self.producer = SimpleProducer(self.kafka)

        self.shard_id = shard
        self.shard_map = shard_map

        # Configure the shard
        if shard is not None:
            assert shard in shard_map
            shard = shard_map[shard]

            self.channel_name = 'votes%s' % self.shard_id
            self.client = Listener(self, self.channel_name)
            print "Listen on: %s" % self.channel_name
        else:
            self.channel_name = 'votes'
            self.client = Listener(self, self.channel_name)

        # Initialize the node for consensus.
        Node.__init__(self, start, quorum, name, shard)

        # Start the subscription loop and log.        
        self.client.start()
        self.RLogger = logging.getLogger()

        print "Init done"

    def __del__(self):
        if self.client:
            self.client.teardown()
            self.kafka.close()

    def send(self, tx, msg):
        if self.shard_map is not None:
            for i in self.shard_map:
                (b0, b1) = self.shard_map[i]
                if within_TX(tx, b0, b1):
                    # self.r.publish('votes:%s' % i , msg)
                    print "Send to: %s " % ('votes%s' % i)
                    self.producer.send_messages('votes%s' % i , msg)
        else:
            self.producer.send_messages('votes' , msg)

    def receive(self, message):
        """ How to process incoming messages. """

        try:
            # Ensure the messae decodes
            message = loads(message)

            # Make sure some basic stuctures are here
            originator = message["from"]
            tx, action = message['Tx'], message['action']
            
            tx = json2Tx(tx)
            idx, deps, new_obj, data = tx

        except Exception as e:
            raise Exception("Badly formatted messages: %s" % str(e))

        # Ignore messages we sent
        if self.name == originator:
            return
        
        if not idx == packageTx(data, deps, len(new_obj))[0]:
            # TODO: Checker goes here.
            raise Exception("Invalid transaction.")

        if not self._within_TX(tx):
            if action == "process":
                # We are going to re-route the message on a correct channel
                msg = dumps({ "action":"process", "from":self.name, "Tx":Tx2json(tx) })
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
            yesno = message['accept']
            self.RLogger.info("Receive commit (%s) for %s (%s)" % (yesno ,idx[:6], self.name))

            ## TODO: call chainer.
            if yesno:
                self.do_commit_yes(tx)
            else:
                self.commit_no.add(idx)

        if action == "process":
            # We process a request
            self.process(tx)


    def on_vote(self, full_tx, vote):
        msg = dumps({ "action":"vote", "from":self.name, "Tx":Tx2json(full_tx), "vote":vote })
        self.send(full_tx, msg)


    def on_commit(self, full_tx, yesno):
        msg = dumps({ "action":"commit", "from":self.name, "Tx":Tx2json(full_tx), "accept":yesno })
        self.send(full_tx, msg)



def test_single():
    kn = KafkaNode(name="node", host=host, port=port)

    rnd = hexlify(urandom(16))
    T0 = packageTx(data="data1,%s" % rnd, deps=[], num_out=3)
    _, _, [A, B, C], txdata = T0

    T0_json = dumps({ "action":"process", "from":"ext", "Tx":Tx2json(T0) })
    kn.send(T0, T0_json)

    T1 = packageTx("333,%s" % rnd, [A, B], 2)
    T2 = packageTx("bbb,%s" % rnd, [A, C], 2)
    T1_json = dumps({ "action":"process", "from":"ext", "Tx":Tx2json(T1) })
    T2_json = dumps({ "action":"process", "from":"ext", "Tx":Tx2json(T2) })

    kn.send(T1, T1_json)
    kn.send(T2, T2_json)

    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        del kn
        print "Clean exit ..."


if __name__ == "__main__":
    # bin/kafka-console-consumer --zookeeper localhost:2181 --topic votes0 --from-beginning

    host = "ec2-54-194-146-93.eu-west-1.compute.amazonaws.com"
    port = 9092

    shard_map = make_shard_map(10)    
    nodes = [ KafkaNode(quorum=1, name="n%s" % i, 
                shard=i, shard_map=shard_map, host=host, port=port) 
                for i in shard_map ]

    def concerned(tx):
        l = []
        for i in shard_map:
            (b0, b1) = shard_map[i]
            if within_TX(tx, b0, b1):
                l.append(i)
        return l

    T0s = []
    for _ in range(100):
        rnd = hexlify(urandom(16))
        T0 = packageTx(data="data1-%s" % rnd, deps=[], num_out=3)
        T0s.append(T0)

        # _, _, [A, B, C], txdata = T0
        # T1 = packageTx("333-%s" % rnd, [A, B], 2)
        # T2 = packageTx("bbb-%s" % rnd, [A, C], 2)

        P = lambda T: dumps({ "action":"process", "from":"ext", "Tx":Tx2json(T) })
        print "T0 depends on: %s" % str(concerned(T0))     

        nodes[0].receive( P(T0) )

    # Relevent Nodes
    # r1nodes = [n for n in nodes if n._within_TX(T1)]
    # r1nodes[0].process(T1)

    # Relevent Nodes
    # r2nodes = [n for n in nodes if n._within_TX(T2)]
    # r2nodes[0].process(T2)

    # def test_condition():
    #    Good = True
    #    Good &= T1[0] in r1nodes[-1].commit_yes
    #    Good &= T1[0] in r1nodes[0].commit_yes
    #    Good &= T2[0] in r2nodes[-1].commit_no
    #    Good &= T2[0] in r2nodes[0].commit_no
    #    assert Good
    #    print "All: %s" % Good

    # t = xTimer(3.0, test_condition)
    # t.start()

    try:
        while True:
            time.sleep(0.1)
    except KeyboardInterrupt:
        # del kn
        print "Clean exit ..."
