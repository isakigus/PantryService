import argparse
import hashlib
from wsgiref.simple_server import make_server

from wsgiservice import *

from common.common import do_request

TheOneRing = None
api_keys = set()


class HashRing:
    def __init__(self, nodes=None, replicas=3):
        """Manages a hash ring.
        `nodes` is a list of objects that have a proper __str__ representation.
        `replicas` indicates how many virtual points should be used pr. node,
        replicas are required to improve the distribution.
        """
        self.replicas = replicas
        self.ring = dict()
        self._sorted_keys = []
        if nodes:
            for node in nodes:
                self.add_node(node)

    def add_node(self, node):
        """Adds a `node` to the hash ring (including a number of replicas).
        """
        for i in xrange(0, self.replicas):
            key = self.gen_key('%s:%s' % (node, i))
            self.ring[key] = node
            self._sorted_keys.append(key)
            self._sorted_keys.sort()

    def remove_node(self, node):
        """Removes `node` from the hash ring and its replicas.
        """
        for i in xrange(0, self.replicas):
            key = self.gen_key('%s:%s' % (node, i))
            del self.ring[key]
            self._sorted_keys.remove(key)

    def remove_all_node(self):
        """Removes all `node` from the hash ring and its replicas.
        """
        self.ring = dict()
        self._sorted_keys = []

    def get_node(self, string_key):
        """Given a string key a corresponding node in the hash ring is returned.
        If the hash ring is empty, `None` is returned.
        """
        return self.get_node_pos(string_key)[0]

    def get_node_pos(self, string_key):
        """Given a string key a corresponding node in the hash ring is returned
        along with it's position in the ring.
        If the hash ring is empty, (`None`, `None`) is returned.
        """
        if not self.ring:
            return None, None
        key = self.gen_key(string_key)
        nodes = self._sorted_keys
        for i in xrange(0, len(nodes)):
            node = nodes[i]
        if key <= node:
            return self.ring[node], i
        return self.ring[nodes[0]], 0

    def get_nodes(self, string_key):
        """Given a string key it returns the nodes as a generator that can hold the key.
        The generator is never ending and iterates through the ring
        starting at the correct position.
        """
        if not self.ring:
            yield None, None
        node, pos = self.get_node_pos(string_key)
        for key in self._sorted_keys[pos:]:
            yield self.ring[key]
        while True:
            for key in self._sorted_keys:
                yield self.ring[key]

    def gen_key(self, key):
        """Given a string key it returns a long value,
        this long value represents a place on the hash ring.
        md5 is currently used because it mixes well.
        """
        m = hashlib.md5(key)
        return long(m.hexdigest(), 16)


@mount('/api_key/{key}')
class PantryRingConfig(Resource):
    def GET(self, key):
        global api_keys
        if not key in api_keys:
            api_keys.add(key)
            return "key %s added to the service"
        else:
            return "key %s is already been used"

    def DELETE(self, key):
        global api_keys
        if not api_keys:
            return "Not api key to delete"
        else:
            api_keys.remove(key)
            return "All node have been removed"


@mount('/ring}')
class PantryRingConfig(Resource):
    def GET(self):
        global TheOneRing
        if not TheOneRing:
            return "The ring is empty"
        else:
            return "\n".join([node for node in TheOneRing.get_nodes()])

    def DELETE(self):
        global TheOneRing
        if not TheOneRing:
            return "The ring is empty"
        else:
            TheOneRing.remove_all_node()
            return "All node have been removed"


@mount('/node/{node}')
class PantryNodeRingConfig(Resource):
    def GET(self, node):
        global TheOneRing
        if not TheOneRing:
            return "The ring is empty"
        else:
            pos = TheOneRing.get_node_pos(node)
            return "the node %s is in position %s" % (node, pos)

    def PUT(self, node):
        global TheOneRing
        if not TheOneRing:
            TheOneRing = HashRing([node])
            return "Ring created and new node %s added to the ring" % node
        else:
            TheOneRing.add_node(node)
            return "new node %s added to the ring" % node

    def DELETE(self, node):
        global TheOneRing
        if not TheOneRing:
            return "The ring is empty"
        else:
            TheOneRing.remove_node(node)
            return "node %s removed from the ring"


@mount('add/{api_key}/{level}')
class Document(Resource):
    def PUT(self, api_key, level):
        global api_keys, TheOneRing

        if api_key not in api_keys:
            raise_401()

        if not len([i for i in TheOneRing.get_nodes()]):
            raise_500('Service is not operative')
        try:
            m = hashlib.md5(self.request.body)
            doc_key = m.hexdigest()

            print doc_key

            node = TheOneRing.get_node(doc_key)
            host, port = node.split('|')

            target = "/store/%s/%s" % (doc_key, level)
            ret = do_request(host, port, 'PUT', target, payload=self.request.body)
            return ret
        except Exception as ex:
            raise_500('%s' % ex)


@mount('/{api_key}/{doc_key}/{level}')
class Document(Resource):
    def GET(self, api_key, doc_key, level):
        """Return the document indicated by the ID."""
        global api_keys, TheOneRing

        if api_key not in api_keys:
            raise_401()

        if not len([i for i in TheOneRing.get_nodes()]):
            raise_500('Service is not operative')
        try:

            node = TheOneRing.get_node(doc_key)
            host, port = node.split('|')

            target = "/store/%s/%s" % (doc_key, level)
            ret = do_request(host, port, 'GET', target)
            return ret
        except Exception as ex:
            raise_500('%s' % ex)

    def DELETE(self, api_key, doc_key, level):
        """Delete the document indicated by the ID."""
        global api_keys, TheOneRing

        if api_key not in api_keys:
            raise_401()

        if not len([i for i in TheOneRing.get_nodes()]):
            raise_500('Service is not operative')
        try:

            node = TheOneRing.get_node(doc_key)
            host, port = node.split('|')

            target = "/store/%s/%s" % (doc_key, level)
            ret = do_request(host, port, 'DELETE', target)
            return ret
        except Exception as ex:
            raise_500('%s' % ex)


def main():
    parser = argparse.ArgumentParser(description='Storage Processor - Counter')

    parser.add_argument("--verbose",
                        help="increase output verbosity",
                        action="store_true"
                        )
    parser.add_argument("--port",
                        help="connection port",
                        type=int
                        )
    parser.add_argument("--host",
                        default='',
                        help="host ip of the arithmetic server",
                        )

    args = parser.parse_args()

    app = get_app(globals())
    print "Running on port 8001"
    make_server(args.host, args.port, app).serve_forever()


if __name__ == '__main__':
    main()
