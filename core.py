from twisted.internet.protocol import Factory, Protocol
from twisted.internet.endpoints import TCP4ServerEndpoint
from twisted.internet import reactor

from StringIO import StringIO

import msgpack

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
        pass

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


## ----------- MAIN ------------ ##

if __name__ == "__main__":
    endpoint = TCP4ServerEndpoint(reactor, 8007)
    endpoint.listen(RcoreFactory("configurable quote"))
    reactor.run()