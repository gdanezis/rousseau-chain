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


def main():
    ## Pass the kafka_url, e.g. `192.168.1.110:9092`
    kafka_url = sys.argv[1]

    ## Register to read messages from the "rousseau" list
    consumer = KafkaConsumer('rousseau',
                             group_id='my_group',
                             bootstrap_servers=[kafka_url])

    ## Register to send to the rousseau-chain channel
    kafka = KafkaClient(kafka_url)
    producer = SimpleProducer(kafka)

    # Initialize a chain backed by 2 disk files
    c = chain(diskHashList("fentries.dat"), diskHashList("fnodes.dat"))

    ## The main even loop
    for message in consumer:
        # message value is raw byte string -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                             message.offset, message.key,
                                             message.value))

        seq = c.add(message.value)
        response = "%s|%s|%s" % (seq, hexlify(c.head()), message.value)
        print (response)

        # Note that the application is responsible for encoding messages to type bytes
        producer.send_messages(b'rousseau-chain', response)

if __name__ == "__main__":
    main()