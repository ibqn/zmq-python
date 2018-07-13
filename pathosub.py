#
# Pathological subscriber
# Subscribes to one random topic and prints received messages
#

import sys
import time

from random import randint

import zmq


def main(url=None):
    ctx = zmq.Context.instance()
    subscriber = ctx.socket(zmq.SUB)
    if url is None:
        url = "tcp://localhost:5556"
    subscriber.connect(url)

    subscription = f"{randint(0, 999):03d}".encode()
    subscriber.setsockopt(zmq.SUBSCRIBE, subscription)

    while True:
        try:
            [topic, data] = subscriber.recv_multipart()
        except KeyboardInterrupt:
            print('Interrupted')
            sys.exit(0)
        assert topic == subscription
        print(data.decode())


if __name__ == '__main__':
    main(sys.argv[1] if len(sys.argv) > 1 else None)
