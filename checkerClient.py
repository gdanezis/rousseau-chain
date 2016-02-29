
from twisted.internet.protocol import Factory, Protocol, ClientFactory
from twisted.internet.endpoints import TCP4ClientEndpoint
from twisted.internet import reactor
from twisted.internet.defer import Deferred, AlreadyCalledError

from core import MsgPackProtocol

class CheckerClientProtocol(MsgPackProtocol):

    def msgReceived(self, msg):
        ''' Process incoming messages '''
        self.transport.loseConnection()
        self.dout.callback(msg)

    def connectionMade(self):
        pass

    def connectionLost(self, reason):
        try:
            self.dout.errback(Exception("Connection Lost"))
        except AlreadyCalledError:
            pass

class CheckerClientFactory(ClientFactory):
    protocol = CheckerClientProtocol

    def __init__(self):
        pass
        
    def connect(self, point, msg):
        d = point.connect(self)
        dout = Deferred()

        def gotprotocol(proto):
            proto.dout = dout
            proto.msgSend(msg)

        d.addCallback(gotprotocol)
        d.addErrback(dout.errback)
        return dout

    def clientConnectionLost(self, connector, reason):
        print("Connection refused ...")
        print (reason)
        reactor.stop()

    def clientConnectionFailed(self, connector, reason):
        print("Connection failed ...")
        print (reason)
        reactor.stop()

if __name__ == "__main__":
    def printmsg(msg):
        print msg

    def gotError(err):
        print("Error: %s" % err.value)

    class crt:
        c = 0

        def p1(self, x):
            self.c +=1 
            if self.c == 10:    
                reactor.stop() 

    if __name__ == "__main__":
        c = crt()
        f = CheckerClientFactory()
        for _ in range(10):
            point = TCP4ClientEndpoint(reactor, "localhost", 9192,)
            d = f.connect(point, {"action":"ping"})
            d.addCallback(printmsg)
            d.addErrback(gotError)
            d.addBoth(c.p1)

        reactor.run()

