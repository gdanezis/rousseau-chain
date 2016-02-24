from StringIO import StringIO
from binascii import hexlify
from hashlib import sha256

from twisted.internet.protocol import Factory, Protocol
from twisted.internet.endpoints import TCP4ServerEndpoint
from twisted.internet import reactor

import msgpack
import redis


class HChain():

    def __init__(self, name):
        ''' Initialize the chain, with a name. '''

        # Remember our names
        self.name = name
        self.namehead = "%s-head" % name

        # Connect to the datbase
        self.r = redis.StrictRedis(host='localhost', port=6379, db=0)
        self._head = self.r.get(self.namehead)
        if self._head is None:
            self._head = sha256(self.namehead).digest()
            self.r.set(self.namehead, self._head)

    def head(self):
        ''' Returns the current head of the chain. '''

        return self._head

    def seal(self, hobject):
        ''' Seals an object into the chain, and returns its hash. '''

        # Build and store the object itself.
        hbody = msgpack.packb(hobject)
        hbodyhash = sha256(hbody).digest()
        self.r.set(hbodyhash, hbody)

        # Build and store the next hash block
        hblock = msgpack.packb([self._head , hbodyhash ])
        hblockhash = sha256(hblock).digest()
        self.r.set(hblockhash, hblock)

        # Record new head
        self._head = hblockhash
        self.r.set(self.namehead, self._head)

        return hbodyhash

class MsgPackProtocol(Protocol):
    def connectionMade(self):
        pass

    def dataReceived(self, data):
        ''' Decerialize incoming messages '''
        try:
            unpacker = msgpack.Unpacker(StringIO(data))
            for unpacked in unpacker:
                self.msgReceived(unpacked)
        except:
            self.transport.loseConnection()

    def msgSend(self, msg):
        ''' Serialize the message in msgpack format '''
        self.transport.write(msgpack.packb(msg))


    def msgReceived(self, msg):
        pass

class Rcore(MsgPackProtocol):

    def msgReceived(self, msg):
        ''' Process incoming messages '''

        # Detect an action
        if "action" not in msg:
            self.msgSend({"code":"Error", "msg":"Missing action."})
            return

        # Switch by action
        action = msg["action"]
        if action == "info":
            self.do_info(msg)
            return

        elif action == "seal":
            self.do_seal(msg)
            return

        elif action == "head":
            self.do_head(msg)
            return

        else:
            # Unknown action
            self.msgSend({"code":"Error", "msg":"Unknown action."})
            return


    def do_head(self, msg):
        ''' Returns the current head of the chain. '''

        # Return the head of the chain.
        resp = {"code":"OK", "head":self.factory.chain.head()}
        self.msgSend(resp)
        return

    def do_seal(self, msg):
        ''' Seals an onject into the chain, and return current head. '''
        
        # If there is no object, return an error.
        if "object" not in msg:
            resp = {"code":"Error", "msg":"No object found (in seal function)"}
            self.msgSend(resp)
            return

        # TODO: Add checks here.
        # - Extract the checker service, and ensure it is running.
        # - Extract all the dependencies.
        # - Gather the evidence into a bundle.
        # - Submit to checker, and get the response.
        # - Either seal or reject.


        # Seal the object
        hbodyhash = self.factory.chain.seal(msg["object"])        

        # Respond
        resp = {"code":"OK", "head":self.factory.chain.head(), "hobject":hbodyhash}
        self.msgSend(resp)
        return


class RcoreFactory(Factory):

    # This will be used by the default buildProtocol to create new protocols:
    protocol = Rcore

    def __init__(self, name):
        self.chain = HChain(name)

## ----------- TESTS ------------ ##

from twisted.test import proto_helpers
import pytest

def _flushDB():
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    r.flushdb()


@pytest.fixture
def tfactory():
    ''' Create a RCore Protocol ready to test '''
    factory = RcoreFactory('test1')
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

def test_do_head(tfactory):
    _flushDB()

    proto, tr = tfactory
    ser = {"action":"head"}

    # Ensure sending encoding works
    proto.msgReceived(ser)
    unpacker = msgpack.Unpacker(StringIO(tr.value()))
    unpacker = list(unpacker)
    assert len(unpacker) == 1

    for unpacked in unpacker:
        assert "head" in unpacked
        assert unpacked["code"] == "OK"
        assert unpacked["head"] == proto.factory.chain.head()

    _flushDB()

def test_do_seal(tfactory):
    _flushDB()

    proto, tr = tfactory
    ser = {"action":"seal", "object":["Hello", "World"]}

    # Ensure sending encoding works
    proto.msgReceived(ser)
    unpacker = msgpack.Unpacker(StringIO(tr.value()))
    unpacker = list(unpacker)
    assert len(unpacker) == 1

    for unpacked in unpacker:
        assert "head" in unpacked
        assert "hobject" in unpacked
        assert unpacked["code"] == "OK"
        assert unpacked["head"] == proto.factory.chain.head()

    _flushDB()


def test_flushDB():
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    r.set("x", "y")
    assert r.get("x") == "y"
    _flushDB()
    assert r.get("x") == None

def test_chain():
    _flushDB()

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

    _flushDB()


## ----------- MAIN ------------ ##

if __name__ == "__main__":
    endpoint = TCP4ServerEndpoint(reactor, 8007)
    endpoint.listen(RcoreFactory("name"))
    reactor.run()