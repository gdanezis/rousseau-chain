from kafka import SimpleProducer, KafkaClient
from kafka import KafkaConsumer

import logging

import sys

logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    level=logging.WARNING
)

kafka = KafkaClient('192.168.1.142:9092')
producer = SimpleProducer(kafka)

# To consume messages
consumer = KafkaConsumer('rousseau-chain',
                         group_id='my_group',
                         bootstrap_servers=['192.168.1.142:9092'])


# Note that the application is responsible for encoding messages to type bytes
producer.send_messages(b'rousseau', sys.argv[1])


for message in consumer:
    # message value is raw byte string -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print("%s" % message.value)
    break