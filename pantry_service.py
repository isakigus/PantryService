import argparse
import os
from wsgiref.simple_server import make_server

from wsgiservice import *

tier_memmory_mapping = {}

class KeyValueStore(object):
    """Simplistic key-value store that uses main memory as the storage
    backend."""

    def __init__(self):
        self._store = {}

    def unset(self, key):
        """Store the provided `value` for the provided `key`."""
        del self._store[key]

    def set(self, key, value):
        """Store the provided `value` for the provided `key`."""
        self._store[key] = value

    def get(self, key):
        """Return the stored value for the provided `key` or `None` if no
        such key is present."""
        ret = self._store.get(key)
        return ret

    def __str__(self):
        return 'MemmoryStore'


class KeyFileStore(object):
    """Simplistic key-value store that uses a disk in the system as the storage
   backend."""

    def __init__(self, root_dir):
        self._store = {}
        self.root_dir = root_dir

    def set(self, key, value):
        """Store the provided `value` for the provided `key`."""
        with open(os.path.join(self.root_dir, key), 'w') as store:
            store.write(value)

    def get(self, key):
        """Return the stored value for the provided `key` or `None` if no
        such key is present."""
        with open(os.path.join(self.root_dir, key), 'r') as store:
            ret = store.read()
        return ret

    def unset(self, key):
        """Return the stored value for the provided `key` or `None` if no
        such key is present."""
        os.remove(os.path.join(self.root_dir, key))

    def __str__(self):
        return 'DiskStore pointing at -> %s' % self.root_dir


@mount('/store_mapping')
class StorageMappingView(Resource):
    """Api call that inform about the current tier mapping storage of the node."""
    def GET(self):
        out = ''
        for k, v in tier_memmory_mapping.items():
            out += "level:%s -> %s\n" % (k, v)
        return out


@mount('/store_config/{level}/{route}')
class StorageConfig(Resource):
    """Api calls used to set up the tier mapping storage of the node.
       Diferente storage devices can be mapped to diferent logical address."""
    def PUT(self, level, route):
        """Insert a new route/device into the node mapping"""
        global tier_memmory_mapping
        if not level in tier_memmory_mapping:
            if level == '0':
                store_method = KeyValueStore()
            else:
                store_method = KeyFileStore(route)
            tier_memmory_mapping[level] = store_method
            msg = "new route %s , level %s added to storgare mapping" % (level, route)
            raise_201(self, msg)
        else:
            msg = "route already exists in the mapping"
            raise_500(self, msg)

    def DELETE(self, level):
        """Remove device from the node mapping"""
        global tier_memmory_mapping
        if level in tier_memmory_mapping:
            del tier_memmory_mapping[level]
            return 'route %s deleted'
        else:
            raise_500(self, 'no route found')


@mount('/store/{key}/{level}')
class StorageService(Resource):
    """Document storage api, it is an interface for storing docuements send by
       the counter service.
    """
    def GET(self, key, level):
        """
        Retrieves the document content indetify by `key` stored in device `level`
        """
        try:
            return tier_memmory_mapping[level].get(key)
        except Exception as ex:
            raise_500(self, 'No document found')

    def DELETE(self, key, level):
        """
        Removes the document indetify by `key` stored in device `level`
        """
        try:
            tier_memmory_mapping[level].unset(key)
            return 'document %s deleted' % key
        except Exception as ex:
            print ex
            raise_500(self, 'No document found')

    def PUT(self, key, level):
        """
        Saves in device `level` the document contnet indetify by `key`
        """
        try:
            value = self.request.body
            msg = tier_memmory_mapping[level].set(key, value)
            raise_201(self, msg)
        except Exception as ex:
            print ex
            raise_500(self, 'Document not saved')


def main():
    parser = argparse.ArgumentParser(description='Storage service - Pantry')

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
