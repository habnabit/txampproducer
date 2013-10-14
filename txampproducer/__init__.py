import uuid

from twisted.internet.interfaces import IConsumer
from twisted.internet import defer
from twisted.protocols.amp import AMP, Argument, Boolean, Command, String
from twisted.python import log
from zope.interface import implementer


class _RequestSomeData(Command):
    arguments = [('id', String())]
    response = [
        ('data', String()),
        ('more', Boolean(optional=True)),
    ]


class _PushSomeData(Command):
    arguments = [
        ('id', String()),
        ('data', String()),
        ('done', Boolean(optional=True)),
    ]
    response = []


class _ToggleSendingData(Command):
    arguments = [
        ('id', String()),
        ('keepGoing', Boolean()),
    ]
    response = []


class _ProducerDone(Command):
    arguments = [('id', String())]
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

    @_ProducerDone.responder
    def _producerDone(self, id):
        producer = self._producers.pop(id)
        producer.unregisterConsumer()
        return {}

    @_PushSomeData.responder
    def _pushedData(self, id, data, done=False):
        self._producers[id]._gotData(data, False)
        if done:
            self._producerDone(id)
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
        if not ret.get('more') and id not in self._producers:
            kw['done'] = True
        d = self.callRemote(_PushSomeData, id=id, data=ret['data'], **kw)
        if ret.get('more'):
            d.addCallback(lambda ign: self._gotConsumerData(id, isPush, None))
        d.addErrback(log.err, 'error pushing data')

    def _lostConsumerProducer(self, id):
        del self._producers[id]
        del self._consumers[id]
        if id in self._buffers:
            return
        d = self.callRemote(_ProducerDone, id=id)
        d.addErrback(log.err, 'error notifying of producer completion')

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


class _AMPProducer(object):
    def __init__(self, id, proto, isPush):
        self._id = id
        self._proto = proto
        self._isPush = isPush
        self._lock = defer.DeferredLock()

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

    def registerConsumer(self, consumer):
        self._consumer = consumer
        self._consumer.registerProducer(self, self._isPush)

    def unregisterConsumer(self):
        self._consumer.unregisterProducer()
        self._consumer = None


class Producer(Argument):
    def __init__(self, optional=False, maxStringPartSize=8192):
        Argument.__init__(self, optional)
        self.maxStringPartSize = maxStringPartSize

    def toStringProto(self, makeProducer, proto):
        id = uuid.uuid4().bytes
        consumer = _AMPConsumer(id, proto)
        producer = makeProducer(consumer)
        if consumer._isPush is None:
            raise ValueError("makeProducer callable didn't register a producer")
        proto._producers[id] = producer
        proto._consumers[id] = consumer
        return id + ('y' if consumer._isPush else 'n')

    def fromStringProto(self, id, proto):
        id, isPush = id[:-1], id[-1] == 'y'
        producer = _AMPProducer(id, proto, isPush)
        proto._producers[id] = producer
        return producer