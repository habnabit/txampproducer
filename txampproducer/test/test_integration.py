from StringIO import StringIO

from twisted.internet.interfaces import IConsumer
from twisted.internet.task import Clock, LoopingCall
from twisted.protocols.amp import Command
from twisted.protocols.basic import FileSender
from twisted.test.testutils import returnConnected
from zope.interface import implementer

from txampproducer import ProducerAMP, Producer


class SendProducer(Command):
    arguments = [('producer', Producer())]
    response = []


class TestAMP(ProducerAMP):
    @SendProducer.responder
    def receiveProducer(self, producer):
        self.producer = producer
        return {}


@implementer(IConsumer)
class FileConsumer(object):
    _looper = None
    _producer = None

    def __init__(self, reactor=None, interval=0.01):
        self._reactor = reactor
        self._interval = interval
        self._io = StringIO()

    def registerProducer(self, producer, isPush):
        if not isPush and not self._reactor:
            raise ValueError("can't read from a pull producer with no reactor")
        self._producer = producer
        if isPush:
            raise NotImplementedError()
        else:
            self._looper = LoopingCall(self._pull)
            self._looper.clock = self._reactor
            self._looper.start(self._interval, now=False)

    def unregisterProducer(self):
        self._producer = None
        if self._looper:
            self._looper.stop()

    def _pull(self):
        self._producer.resumeProducing()

    def write(self, data):
        self._io.write(data)

    def value(self):
        return self._io.getvalue()


def test_pullFileConsumer():
    fileToSend = StringIO('spam eggs' * 10240)
    clock = Clock()
    consumer = FileConsumer(clock)
    d = FileSender().beginFileTransfer(fileToSend, consumer)
    finished = []
    d.addCallback(finished.append)
    while not finished:
        clock.advance(1)
    assert consumer.value() == fileToSend.getvalue()


def test_pullProducer():
    client = TestAMP()
    server = TestAMP()
    pump = returnConnected(server, client)
    fileToSend = StringIO('spam eggs ' * 10240)
    clock = Clock()
    consumer = FileConsumer(clock)
    def registerWithConsumer(consumer):
        producer = FileSender()
        producer.beginFileTransfer(fileToSend, consumer)
        return producer
    client.callRemote(SendProducer, producer=registerWithConsumer)
    pump.pump()
    server.producer.registerConsumer(consumer)
    while True:
        clock.advance(1)
        if not pump.pump():
            break
    assert consumer.value() == fileToSend.getvalue()
