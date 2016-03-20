from kafka import SimpleProducer, KafkaClient
from kafka import KafkaConsumer

from chain import chain
from store import diskHashList

import logging
from binascii import hexlify

import sys

logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    level=logging.WARNING
)


from consensusim import Node
from json import dumps, loads

def main():
    ## Pass the kafka_url, e.g. `192.168.1.110:9092`
    kafka_url = sys.argv[1]
    # channel = sys.argv[2]

    ## Register to read messages from the "rousseau" list
    consumer = KafkaConsumer('shards',
                             group_id='my_group',
                             bootstrap_servers=[kafka_url])

    ## Register to send to the rousseau-chain channel
    kafka = KafkaClient(kafka_url)
    producer = SimpleProducer(kafka)
    # producer.send_messages(b'shards', "Hello")


    # Initialize a chain backed by 2 disk files
    c = chain(diskHashList("fentries.dat"), diskHashList("fnodes.dat"))

    class RousseauNode(Node):
        def on_commit(self, tx, yesno):
            obj = dumps([tx, yesno])

            seq = c.add(obj)
            producer.send_messages(b'rousseau', dumps([obj, seq, hexlify(c.head())]) )

            ## Commit to DB the chain.

    # The consistency engine
    n = RousseauNode()

    ## The main even loop
    for message in consumer:
        # message value is raw byte string -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                             message.offset, message.key,
                                             message.value))

        try:
            idx, deps, new_objs = loads(message.value)

            ## 1) Custom checker.
            ## 2) Store into DB.

            print("ID: %s Deps: %s New: %s" % (idx, str(deps), str(new_objs)))

            n.process((idx, deps, new_objs))
        except:
            print("Cannot decode: %s" % message.value)
        

        # Note that the application is responsible for encoding messages to type bytes
        # producer.send_messages(b'shards', response)

if __name__ == "__main__":
    main()