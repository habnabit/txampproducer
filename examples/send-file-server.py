import hashlib
import sys

from twisted.internet import defer, endpoints, protocol, task

from txampproducer import ProducerAMP, SendProducer, deliverContent


class HasherProducerAMP(ProducerAMP):
    @SendProducer.responder
    def receiveProducer(self, producer, name):
        d = deliverContent(producer)
        d.addCallback(self._gotData, name)
        return {}

    def _gotData(self, data, name):
        hashed = hashlib.sha1(data).hexdigest()
        print name, hashed


class HasherProducerAMPFactory(protocol.Factory):
    protocol = HasherProducerAMP


def main(reactor, description):
    endpoint = endpoints.serverFromString(reactor, description)
    d = endpoint.listen(HasherProducerAMPFactory())
    d.addCallback(lambda port: defer.Deferred())
    return d


task.react(main, sys.argv[1:])
