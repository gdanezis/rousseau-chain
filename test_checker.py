from StringIO import StringIO
from binascii import hexlify
from hashlib import sha256

from twisted.internet.protocol import Factory, Protocol
from twisted.internet.endpoints import TCP4ClientEndpoint
from twisted.internet import reactor

import msgpack

from core import MsgPackProtocol

class CheckerTestProtocol(MsgPackProtocol):

    def msgReceived(self, msg):
        ''' Process incoming messages '''
        print(msg)
        self.transport.loseConnection()
        reactor.stop()


class CheckerTestFactory(Factory):
    protocol = CheckerTestProtocol
        
def gotProtocol(proto):
    proto.msgSend({"action":"ping"})
    
def gotError(err):
    raise err

if __name__ == "__main__":
    point = TCP4ClientEndpoint(reactor, "localhost", 9192)
    d = point.connect(CheckerTestFactory())
    d.addCallback(gotProtocol)
    d.addErrback(gotError)
    reactor.run()

