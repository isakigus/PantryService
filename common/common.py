import httplib


def do_request(host, port, verb, uri, payload=''):
    conn = httplib.HTTPConnection(host, port)
    conn.request(verb, uri, payload)
    response = conn.getresponse()
    return response.status, response.reason, response.read()
