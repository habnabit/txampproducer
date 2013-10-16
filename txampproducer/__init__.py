from twisted.internet.interfaces import IConsumer, IProducer, IPushProducer, IPullProducer
from twisted.internet import defer, protocol
from twisted.protocols.amp import AMP, Argument, Boolean, Command, Integer, String
from twisted.python import failure, log
from twisted.web.client import FileBodyProducer
from zope.interface import directlyProvides, implementer, Interface


class _RequestSomeData(Command):
    arguments = [('id', Integer())]
    response = [
        ('data', String()),
        ('more', Boolean(optional=True)),
    ]


class _PushSomeData(Command):
    arguments = [
        ('id', Integer()),
        ('data', String()),
        ('done', Boolean(optional=True)),
    ]
    response = []


class _ToggleSendingData(Command):
    arguments = [
        ('id', Integer()),
        ('keepGoing', Boolean()),
    ]
    response = []


class _ProducerDone(Command):
    arguments = [('id', Integer())]
    response = []


class Producer(Argument):
    def __init__(self, optional=False, maxStringPartSize=8192):
        Argument.__init__(self, optional)
        self.maxStringPartSize = maxStringPartSize

    def toStringProto(self, makeProducer, proto):
        id = proto._generateProducerID()
        consumer = _AMPConsumer(id, proto)
        d = proto._notifyFinishSending(id)
        makeProducer(consumer, d)
        if consumer._isPush is None:
            raise ValueError("makeProducer callable didn't register a producer")
        proto._producers[id] = consumer._producer
        proto._consumers[id] = consumer
        return str(id) + ('y' if consumer._isPush else 'n')

    def fromStringProto(self, id, proto):
        id, isPush = id[:-1], id[-1] == 'y'
        id = int(id)
        producer = _AMPProducer(id, proto, isPush)
        proto._producers[id] = producer
        return producer


class IProducerStarter(Interface):
    pass


@implementer(IProducerStarter)
class ProducerStarter(object):
    def __init__(self, registerWithConsumer):
        self.deferred = None
        self._registerWithConsumer = registerWithConsumer

    def __call__(self, consumer, deferred):
        self.deferred = deferred
        self._registerWithConsumer(consumer)


class SendProducer(Command):
    arguments = [
        ('name', String(optional=True)),
        ('producer', Producer()),
    ]
    response = []


class ProducerAMP(AMP):
    _nextProducerID = 0

    def __init__(self, *a, **kw):
        AMP.__init__(self, *a, **kw)
        self._producers = {}
        self._consumers = {}
        self._buffers = {}
        self._pending = {}
        self._waitingOnCompletion = {}
        self._draining = set()

    def _generateProducerID(self):
        self._nextProducerID += 1
        return self._nextProducerID

    @_RequestSomeData.responder
    def _dataRequested(self, id):
        assert id not in self._pending
        d = defer.Deferred()
        if id not in self._buffers:
            self._pending[id] = d
            self._producers[id].resumeProducing()
        else:
            self._pumpBuffer(id, deferred=d)
        return d

    @_ProducerDone.responder
    def _producerDone(self, id):
        self._finishReceiving(id)
        return {}

    @_PushSomeData.responder
    def _pushedData(self, id, data, done=False):
        self._producers[id]._gotData(data, False)
        if done:
            self._finishReceiving(id)
        return {}

    @_ToggleSendingData.responder
    def _dataStateToggled(self, id, keepGoing):
        producer = self._producers[id]
        if keepGoing:
            producer.resumeProducing()
        else:
            producer.pauseProducing()
        return {}

    def _gotConsumerData(self, id, isPush, data):
        ret = self._pumpBuffer(id, data=data, returnContents=isPush)
        if not isPush or not ret or not ret['data']:
            return
        kw = {}
        if not ret.get('more') and id in self._draining:
            kw['done'] = True
        d = self.callRemote(_PushSomeData, id=id, data=ret['data'], **kw)
        if ret.get('more'):
            d.addCallback(lambda ign: self._gotConsumerData(id, isPush, None))
        elif id in self._draining:
            self._finishSending(id)
        d.addErrback(log.err, 'error pushing data')

    def _lostConsumerProducer(self, id):
        if id in self._buffers:
            self._draining.add(id)
            return
        d = self.callRemote(_ProducerDone, id=id)
        d.addErrback(log.err, 'error notifying of producer completion')
        self._finishSending(id)

    def _finishSending(self, id, value=None):
        del self._producers[id]
        del self._consumers[id]
        self._draining.discard(id)
        waiting = self._waitingOnCompletion.pop(id, [])
        for d in waiting:
            d.callback(value)

    def _finishReceiving(self, id, reason=None):
        if reason is None:
            reason = failure.Failure(TransferDone())
        producer = self._producers.pop(id)
        producer._connectionLost(reason)
        producer.unregisterConsumer()

    def _pumpBuffer(self, id, deferred=None, data=None, returnContents=False):
        bufferList = self._buffers.pop(id, [])
        if data:
            bufferList.append(data)
        if deferred is None:
            deferred = self._pending.pop(id, None)
        if deferred is None and not returnContents:
            self._buffers[id] = bufferList
            return

        bufferString = bufferList[0] if len(bufferList) == 1 else ''.join(bufferList)
        ret = {'data': bufferString[:4096]}
        if len(bufferString) > 4096:
            self._buffers[id] = [bufferString[4096:]]
            ret['more'] = True
        if returnContents:
            return ret
        else:
            deferred.callback(ret)

    def _needProducerData(self, id):
        return self.callRemote(_RequestSomeData, id=id)

    def _toggleSendingData(self, id, keepGoing):
        return self.callRemote(_ToggleSendingData, id=id, keepGoing=keepGoing)

    def _notifyFinishSending(self, id):
        d = defer.Deferred()
        self._waitingOnCompletion.setdefault(id, []).append(d)
        return d

    def sendFile(self, fobj, name=None, cooperator=None):
        kw = {}
        if cooperator is not None:
            kw['cooperator'] = cooperator
        @ProducerStarter
        def registerWithConsumer(consumer):
            producer = FileBodyProducer(fobj, **kw)
            d = producer.startProducing(consumer)
            d.addCallback(lambda ign: consumer.unregisterProducer())
            d.addErrback(log.err, 'error producing file body')
            consumer.registerProducer(producer, True)
        d = self.callRemote(SendProducer, producer=registerWithConsumer, name=name)
        d.addCallback(lambda ign: registerWithConsumer.deferred)
        return d

    def connectionLost(self, reason):
        for id, producer in self._producers.items():
            consumer = self._consumers.get(id)
            if consumer is None:
                self._finishReceiving(id, reason)
            else:
                self._finishSending(id, reason)
        AMP.connectionLost(self, reason)


@implementer(IConsumer)
class _AMPConsumer(object):
    _producer = None
    _isPush = None

    def __init__(self, id, proto):
        self._id = id
        self._proto = proto

    def registerProducer(self, producer, isPush):
        self._producer = producer
        self._isPush = isPush

    def unregisterProducer(self):
        self._producer = None
        self._proto._lostConsumerProducer(self._id)

    def write(self, data):
        self._proto._gotConsumerData(self._id, self._isPush, data)


class IAMPProducer(Interface):
    pass


@implementer(IProducer)
@implementer(IAMPProducer)
class _AMPProducer(object):
    def __init__(self, id, proto, isPush):
        self._id = id
        self._proto = proto
        self._isPush = isPush
        self._lock = defer.DeferredLock()
        if self._isPush:
            directlyProvides(self, IPushProducer)
        else:
            directlyProvides(self, IPullProducer)
        self._deliveryProto = None

    def pauseProducing(self):
        if not self._isPush:
            raise ValueError('only push producers can be paused')
        d = self._lock.run(self._proto._toggleSendingData, self._id, keepGoing=False)
        d.addErrback(log.err, 'error pausing production')

    def resumeProducing(self):
        if self._isPush:
            d = self._lock.run(self._proto._toggleSendingData, self._id, keepGoing=True)
        else:
            d = self._lock.run(self._proto._needProducerData, self._id)
            d.addCallback(lambda d: self._gotData(**d))
        d.addErrback(log.err, 'error resuming production')

    def _gotData(self, data, more):
        self._deferred = None
        self._consumer.write(data)
        if more:
            self.resumeProducing()

    def _connectionLost(self, reason):
        if self._deliveryProto is not None:
            self._deliveryProto.connectionLost(reason)

    def registerConsumer(self, consumer):
        self._consumer = consumer
        self._consumer.registerProducer(self, self._isPush)

    def unregisterConsumer(self):
        self._consumer.unregisterProducer()
        self._consumer = None

    def deliverAsProtocol(self, protocol):
        if not self._isPush:
            raise ValueError('only push producers can be delivered')
        self.registerConsumer(_ProtocolToConsumer(protocol))
        self._deliveryProto = protocol


class TransferDone(Exception):
    pass


@implementer(IConsumer)
class _ProtocolToConsumer(object):
    _producer = None

    def __init__(self, proto):
        self._proto = proto

    def registerProducer(self, producer, isPush):
        if not isPush:
            raise ValueError('only push producers can be delivered')
        self._producer = producer

    def unregisterProducer(self):
        self._producer = None

    def write(self, data):
        self._proto.dataReceived(data)


class _AccretionProtocol(protocol.Protocol):
    def __init__(self):
        self._deferred = defer.Deferred()
        self._buffer = []

    def dataReceived(self, data):
        self._buffer.append(data)

    def connectionLost(self, reason):
        if reason.check(TransferDone):
            self._deferred.callback(''.join(self._buffer))
            self._buffer = None
        else:
            self._deferred.errback(reason)


def deliverContent(producer):
    proto = _AccretionProtocol()
    producer.deliverAsProtocol(proto)
    return proto._deferred
