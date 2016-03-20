from StringIO import StringIO
from binascii import hexlify
from hashlib import sha256

from twisted.internet.protocol import Factory, Protocol
from twisted.internet.endpoints import TCP4ServerEndpoint
from twisted.internet import reactor

import msgpack

from core import MsgPackProtocol

class CheckerProtocol(MsgPackProtocol):

    def msgReceived(self, msg):
        ''' Process incoming messages '''

        # Detect an action
        if "action" not in msg:
            self.msgSend({"code":"Error", "msg":"Missing action."})
            return

        # Switch by action
        action = msg["action"]
        if action == "ping":
            self.do_ping(msg)
            return

        elif action == "check":
            self.do_check(msg)
            return

        else:
            # Unknown action
            self.msgSend({"code":"Error", "msg":"Unknown action."})
            return


    def do_ping(self, msg):
        ''' Responds to a ping to check connectivity. '''

        # Return the head of the chain.
        resp = {"code":"OK", "pong":self.factory.name}
        self.msgSend(resp)
        return

    def do_check(self, msg):
        ''' Checks the transaction is valid. '''
        
        # If there is no object, return an error.
        if "object" not in msg:
            resp = {"code":"Error", "msg":"No object found (in seal function)"}
            self.msgSend(resp)
            return

        resp = {"code":"OK", "valid":True}
        self.msgSend(resp)
        return


class CheckerFactory(Factory):

    # This will be used by the default buildProtocol to create new protocols:
    protocol = CheckerProtocol

    def __init__(self, name):
        self.name = name

## ------------- TESTS -------------

from twisted.test import proto_helpers
import pytest


@pytest.fixture
def tfactory():
    ''' Create a RCore Protocol ready to test '''
    factory = CheckerFactory('test1')
    proto = factory.buildProtocol(('127.0.0.1', 0))
    tr = proto_helpers.StringTransport()
    proto.makeConnection(tr)
    return proto, tr

def test_do_ping(tfactory):
    
    proto, tr = tfactory
    ser = {"action":"ping"}

    # Ensure sending encoding works
    proto.msgReceived(ser)
    unpacker = msgpack.Unpacker(StringIO(tr.value()))
    unpacker = list(unpacker)
    assert len(unpacker) == 1

    for unpacked in unpacker:
        assert "pong" in unpacked
        assert unpacked["pong"] == "test1"

def test_do_check(tfactory):
    
    proto, tr = tfactory
    ser = {"action":"check", "object":"Transaction"}

    # Ensure sending encoding works
    proto.msgReceived(ser)
    unpacker = msgpack.Unpacker(StringIO(tr.value()))
    unpacker = list(unpacker)
    assert len(unpacker) == 1

    for unpacked in unpacker:
        assert "valid" in unpacked
        assert unpacked["valid"] 

        

if __name__ == "__main__":
    endpoint = TCP4ServerEndpoint(reactor, 9191)
    endpoint.listen(CheckerFactory("name"))
    reactor.run()