import argparse
import httplib

"""
python shopper.py --verb PUT --port 12345 --host 127.0.0.1 --data 'The bare necessities of life came to you' --key api_key
"""


def main():
    parser = argparse.ArgumentParser(description='document shopper')

    parser.add_argument("--verb",
                        help="http verb",
                        default="GET",
                        choices=["GET", "POST", "PUT", "DELETE"]
                        )
    parser.add_argument("--port",
                        help="connection port",
                        type=int,
                        default=8998,
                        )
    parser.add_argument("--host",
                        help="service host",
                        default='127.0.01'
                        )
    parser.add_argument("--data",
                        dest="payload",
                        default=None)

    parser.add_argument("--uri",
                        dest="uri",
                        help="uri",
                        required=True
                        )

    args = parser.parse_args()

    conn = httplib.HTTPConnection(args.host, args.port)
    conn.request(args.verb, args.uri, args.payload)
    response = conn.getresponse()
    print response.status, response.reason, response.read()


if __name__ == '__main__':
    main()
