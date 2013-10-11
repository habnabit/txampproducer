import uuid

from twisted.internet.interfaces import IConsumer
from twisted.internet import defer
from twisted.protocols.amp import AMP, Argument, Boolean, Command, String
from zope.interface import implementer


class _RequestSomeData(Command):
    arguments = [('id', String())]
    response = [
        ('data', String()),
        ('more', Boolean()),
    ]


class _PushSomeData(Command):
    arguments = [
        ('id', String()),
        ('data', String()),
    ]
    response = []


class _ToggleSendingData(Command):
    arguments = [
        ('id', String()),
        ('continue', Boolean()),
    ]
    response = []


class ProducerAMP(AMP):
    def __init__(self, *a, **kw):
        AMP.__init__(self, *a, **kw)
        self._producers = {}
        self._consumers = {}
        self._buffers = {}
        self._pending = {}

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

    def _gotConsumerData(self, id, data):
        self._pumpBuffer(id, data=data)

    def _pumpBuffer(self, id, deferred=None, data=None):
        bufferList = self._buffers.pop(id, [])
        if data:
            bufferList.append(data)
        if deferred is None:
            deferred = self._pending.pop(id, None)
        if deferred is None:
            self._buffers[id] = bufferList
            return

        bufferString = bufferList[0] if len(bufferList) == 1 else ''.join(bufferList)
        data = bufferString[:4096]
        more = False
        if len(bufferString) > 4096:
            self._buffers[id] = [bufferString[4096:]]
            more = True
        deferred.callback({'data': data, 'more': more})

    def _needProducerData(self, id):
        return self.callRemote(_RequestSomeData, id=id)


@implementer(IConsumer)
class _AMPConsumer(object):
    def __init__(self, id, proto):
        self._id = id
        self._proto = proto

    def registerProducer(self, producer, isPush):
        self._producer = producer
        self._isPush = isPush

    def unregisterProducer(self):
        self._producer = None

    def write(self, data):
        self._proto._gotConsumerData(self._id, data)


class _AMPProducer(object):
    def __init__(self, id, proto):
        self._id = id
        self._proto = proto
        self._deferred = None

    def resumeProducing(self):
        if self._deferred is not None:
            return
        self._deferred = d = self._proto._needProducerData(self._id)
        d.addCallback(self._gotData)

    def _gotData(self, data):
        self._deferred = None
        self._consumer.write(data['data'])
        if data['more']:
            self.resumeProducing()

    def registerConsumer(self, consumer):
        self._consumer = consumer
        self._consumer.registerProducer(self, False)


class Producer(Argument):
    def __init__(self, optional=False, maxStringPartSize=8192):
        Argument.__init__(self, optional)
        self.maxStringPartSize = maxStringPartSize

    def toStringProto(self, makeProducer, proto):
        id = uuid.uuid4().bytes
        consumer = _AMPConsumer(id, proto)
        producer = makeProducer(consumer)
        proto._producers[id] = producer
        proto._consumers[id] = consumer
        return id

    def fromStringProto(self, id, proto):
        producer = _AMPProducer(id, proto)
        proto._producers[id] = producer
        return producer
