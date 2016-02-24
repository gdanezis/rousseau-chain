from StringIO import StringIO

from twisted.internet.protocol import Factory, Protocol
from twisted.internet.endpoints import TCP4ServerEndpoint
from twisted.internet import reactor

import msgpack
import redis
from hashlib import sha256

from binascii import hexlify

class HChain():

    def __init__(self, name):
        self.name = name
        self.namehead = "%s-head" % name

        self.r = redis.StrictRedis(host='localhost', port=6379, db=0)
        self._head = self.r.get(self.namehead)
        if self._head is None:
            self._head = sha256(self.namehead).digest()
            self.r.set(self.namehead, self._head)

    def head(self):
        return self._head

    def seal(self, hobject):
        # Build and store the object itself.
        hbody = msgpack.packb(hobject)
        hbodyhash = sha256(hbody).digest()
        self.r.set(hbodyhash, hbody)

        # Build and store the next hash block
        hblock = msgpack.packb([self._head , hbodyhash ])
        hblockhash = sha256(hblock).digest()
        self.r.set(hblockhash, hblock)

        self._head = hblockhash
        self.r.set(self.namehead, self._head)

    def __del__(self):
        self.r.quit()


class Rcore(Protocol):

    def connectionMade(self):
        pass

    def dataReceived(self, data):
        try:
            unpacker = msgpack.Unpacker(StringIO(data))
            for unpacked in unpacker:
                self.msgReceived(unpacked)
        except:
            self.transport.loseConnection()

    def msgReceived(self, msg):
        # Detect an action
        if action not in msg:
            self.msgSend({"code":"Error", "msg":"Missing action."})
            return

        # Switch by action
        action = msg["action"]
        if action == "info":
            self.do_info(msg)
            return

        elif action == "add":
            self.do_add(msg)
            return

        elif action == "head":
            self.do_info(head)
            return

        else:
            # Unknown action
            self.msgSend({"code":"Error", "msg":"Unknown action."})
            return


    def msgSend(self, msg):
        ''' Serialize the message in msgpack format '''
        self.transport.write(msgpack.packb(msg))


class RcoreFactory(Factory):

    # This will be used by the default buildProtocol to create new protocols:
    protocol = Rcore

    def __init__(self):
        pass

## ----------- TESTS ------------ ##

from twisted.test import proto_helpers
import pytest

def _flushDB():
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    r.flushdb()


@pytest.fixture
def tfactory():
    ''' Create a RCore Protocol ready to test '''
    factory = RcoreFactory()
    proto = factory.buildProtocol(('127.0.0.1', 0))
    tr = proto_helpers.StringTransport()
    proto.makeConnection(tr)

    return proto, tr

def test_protocol_msgsend(tfactory):
    proto, tr = tfactory
    ser = {"Hello":"World"}

    # Ensure the facory is known
    assert proto.factory
   
    # Ensure sending encoding works
    proto.msgSend(ser)
    unpacker = msgpack.Unpacker(StringIO(tr.value()))
    unpacker = list(unpacker)
    assert len(unpacker) == 1

    for unpacked in unpacker:
        assert unpacked["Hello"] == "World"

def test_protocol_unmarshall(monkeypatch, tfactory):
    # Monkey patch to write to a list
    proto, tr = tfactory
    data = []
    monkeypatch.setattr(proto, "msgReceived", lambda msg: data.append(msg))

    ser = msgpack.packb([{"Hello":"World"}])
    proto.dataReceived(ser)

    assert [{"Hello":"World"}] == data[0]

def test_flushDB():
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    r.set("x", "y")
    assert r.get("x") == "y"
    _flushDB()
    assert r.get("x") == None

def test_chain():

    # Two same chains
    
    h1 = HChain("h1")
    for i in range(10):
        h1.seal(i)

    h1_head = h1.head()
    _flushDB()

    h1 = HChain("h1")
    for i in range(10):
        h1.seal(i)

    h1_head_prime = h1.head()

    assert h1_head == h1_head_prime

    # Memory & persistance in chains
    _flushDB()

    h1 = HChain("h1")
    for i in range(10):
        h1.seal(i)

    intermediteH = h1.head()

    h2 = HChain("h1")
    
    assert h2.head() == intermediteH

    for i in range(10, 20):
        h2.seal(i)

    h1_head = h2.head()
    _flushDB()

    h3 = HChain("h1")
    for i in range(20):
        h3.seal(i)

    h1_head_prime = h3.head()

    assert h1_head == h1_head_prime


## ----------- MAIN ------------ ##

if __name__ == "__main__":
    endpoint = TCP4ServerEndpoint(reactor, 8007)
    endpoint.listen(RcoreFactory("configurable quote"))
    reactor.run()