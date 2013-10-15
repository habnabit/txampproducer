from StringIO import StringIO

from twisted.internet.interfaces import IConsumer, IPushProducer
from twisted.internet.task import Clock, Cooperator, LoopingCall
from twisted.protocols.basic import FileSender
from twisted.python import failure, log
from twisted.test.testutils import returnConnected
from twisted.trial import unittest
from twisted.web.client import FileBodyProducer
from zope.interface import implementer

from txampproducer import ProducerAMP, SendProducer, deliverContent


class TestAMP(ProducerAMP):
    @SendProducer.responder
    def receiveProducer(self, producer, name=None):
        self.producer = producer
        self.fileName = name
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
        if not isPush:
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


@implementer(IPushProducer)
class PausablePushProducer(object):
    def __init__(self):
        self.paused = False

    def pauseProducing(self):
        assert not self.paused
        self.paused = True

    def resumeProducing(self):
        assert self.paused
        self.paused = False


class FakeDisconnectedError(Exception):
    pass


fileData = 'spam eggs' * 10240


class GeneralIntegrationTests(unittest.TestCase):
    def test_pullFileConsumer(self):
        fileToSend = StringIO(fileData)
        clock = Clock()
        consumer = FileConsumer(clock)
        d = FileSender().beginFileTransfer(fileToSend, consumer)
        finished = []
        d.addCallback(finished.append)
        while not finished:
            clock.advance(1)
        self.assertEqual(consumer.value(), fileData)

class ProducerAMPIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.client = TestAMP()
        self.server = TestAMP()
        self.pump = returnConnected(self.server, self.client)

    def test_pullProducer(self):
        fileToSend = StringIO(fileData)
        clock = Clock()
        consumer = FileConsumer(clock)
        def registerWithConsumer(consumer):
            producer = FileSender()
            producer.beginFileTransfer(fileToSend, consumer)
            return producer
        self.client.callRemote(SendProducer, producer=registerWithConsumer)
        self.pump.pump()
        self.server.producer.registerConsumer(consumer)
        while True:
            clock.advance(1)
            if not self.pump.pump():
                break
        self.assertEqual(consumer.value(), fileData)
        self.assertIdentical(consumer._producer, None)

    def test_pushProducer(self):
        fileToSend = StringIO(fileData)
        consumer = FileConsumer()
        clock = Clock()
        cooperator = Cooperator(scheduler=lambda f: clock.callLater(0.1, f))
        def registerWithConsumer(consumer):
            producer = FileBodyProducer(fileToSend, cooperator=cooperator)
            d = producer.startProducing(consumer)
            d.addCallback(lambda ign: consumer.unregisterProducer())
            d.addErrback(log.err, 'error producing file body')
            consumer.registerProducer(producer, True)
            return producer
        self.client.callRemote(SendProducer, producer=registerWithConsumer)
        self.pump.pump()
        self.server.producer.registerConsumer(consumer)

        while self.pump.pump():
            clock.advance(1)
        self.assertEqual(consumer.value(), fileData)
        self.assertIdentical(consumer._producer, None)

    def test_pausingPushProducer(self):
        consumer = FileConsumer()
        producer = PausablePushProducer()
        def registerWithConsumer(consumer):
            consumer.registerProducer(producer, True)
            return producer
        self.client.callRemote(SendProducer, producer=registerWithConsumer)
        self.pump.flush()
        self.server.producer.registerConsumer(consumer)
        self.assertFalse(producer.paused)
        self.server.producer.pauseProducing()
        self.pump.flush()
        self.assertTrue(producer.paused)
        self.server.producer.resumeProducing()
        self.pump.flush()
        self.assertFalse(producer.paused)

    def test_deliverContent(self):
        fileToSend = StringIO(fileData)
        clock = Clock()
        cooperator = Cooperator(scheduler=lambda f: clock.callLater(0.1, f))
        def registerWithConsumer(consumer):
            producer = FileBodyProducer(fileToSend, cooperator=cooperator)
            d = producer.startProducing(consumer)
            d.addCallback(lambda ign: consumer.unregisterProducer())
            d.addErrback(log.err, 'error producing file body')
            consumer.registerProducer(producer, True)
            return producer
        self.client.callRemote(SendProducer, producer=registerWithConsumer)
        self.pump.pump()
        d = deliverContent(self.server.producer)

        while self.pump.pump():
            clock.advance(1)
        self.assertEqual(self.successResultOf(d), fileData)

    def test_sendFile(self):
        fileToSend = StringIO(fileData)
        clock = Clock()
        cooperator = Cooperator(scheduler=lambda f: clock.callLater(0.1, f))
        self.client.sendFile(fileToSend, cooperator=cooperator)
        self.pump.pump()
        d = deliverContent(self.server.producer)

        while self.pump.pump():
            clock.advance(1)
        self.assertEqual(self.successResultOf(d), fileData)

    def test_sendFileDisconnection(self):
        fileToSend = StringIO(fileData)
        clock = Clock()
        cooperator = Cooperator(scheduler=lambda f: clock.callLater(0.1, f))
        self.client.sendFile(fileToSend, cooperator=cooperator)
        self.pump.pump()
        d = deliverContent(self.server.producer)
        self.pump.pump()
        clock.advance(1)
        self.server.connectionLost(failure.Failure(FakeDisconnectedError()))
        self.failureResultOf(d, FakeDisconnectedError)
