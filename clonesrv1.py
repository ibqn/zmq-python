from random import (
    randint,
    seed,
)
import time

import zmq

from kvsimple import KVMsg


def main():
    # Prepare our context and publisher socket
    ctx = zmq.Context()
    publisher = ctx.socket(zmq.PUB)

    publisher.bind("tcp://*:5556")
    time.sleep(0.2)

    sequence = 0
    seed(time.time())
    kvmap = {}

    try:
        while True:
            # Distribute as key-value message
            sequence += 1
            kvmsg = KVMsg(sequence)
            kvmsg.key = f"{randint(1, 10000):d}".encode()
            kvmsg.body = f"{randint(1, 1000000):d}".encode()
            kvmsg.send(publisher)
            kvmsg.store(kvmap)
    except KeyboardInterrupt:
        print(f"Interrupted\n{sequence} messages out")


if __name__ == '__main__':
    main()
