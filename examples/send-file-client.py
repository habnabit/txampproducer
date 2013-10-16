import sys

from twisted.internet import endpoints, task

from txampproducer import ProducerAMP


def main(reactor, description, filename):
    endpoint = endpoints.clientFromString(reactor, description)
    d = endpoints.connectProtocol(endpoint, ProducerAMP())
    d.addCallback(lambda proto: proto.sendFile(open(filename), filename))
    return d


task.react(main, sys.argv[1:])
