from kafka import SimpleProducer, KafkaClient
from kafka import KafkaConsumer

from chain import chain

import logging
from binascii import hexlify

logging.basicConfig(
    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
    level=logging.WARNING
)

class diskHashList:
    def __init__(self, filename):
        self.f = open(filename, "r+b")

    def append(self, value):
        assert type(value) == bytes and len(value) == 32
        self.f.seek(0, 2) # End of the file
        self.f.write(value)
        self.f.flush()

    def __len__(self):
        self.f.seek(0,2) # move the cursor to the end of the file
        size = self.f.tell()
        return size / 32

    def __getitem__(self, key):
        size = len(self)

        if key < 0:
            key = size + key

        assert type(key) == int and 0 <= key
        self.f.seek(32 * key, 0)
        data = self.f.read(32)
        assert len(data) == 32
        return data

    def __del__(self):
        self.f.flush()
        self.f.close()

def main():
    ## Register to read messages from the "rousseau" list
    consumer = KafkaConsumer('rousseau',
                             group_id='my_group',
                             bootstrap_servers=['192.168.1.142:9092'])

    ## Register to send to the rousseau-chain channel
    kafka = KafkaClient('192.168.1.142:9092')
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
        response = "%s head=%s value=%s" % (seq, hexlify(c.head()), message.value)
        print (response)

        # Note that the application is responsible for encoding messages to type bytes
        producer.send_messages(b'rousseau-chain', response)

if __name__ == "__main__":
    main()