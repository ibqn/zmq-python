import time

import zmq

from kvsimple import KVMsg


def main():

    # Prepare our context and subscriber
    ctx = zmq.Context()
    snapshot = ctx.socket(zmq.DEALER)
    snapshot.linger = 0
    snapshot.connect("tcp://localhost:5556")
    subscriber = ctx.socket(zmq.SUB)
    subscriber.linger = 0
    subscriber.setsockopt(zmq.SUBSCRIBE, b'')
    subscriber.connect("tcp://localhost:5557")

    kvmap = {}

    # Get state snapshot
    sequence = 0
    snapshot.send(b"ICANHAZ?")
    while True:
        try:
            kvmsg = KVMsg.recv(snapshot)
        except:
            break  # Interrupted

        if kvmsg.key == b"KTHXBAI":
            sequence = kvmsg.sequence
            print(f"Received snapshot={sequence:d}")
            break  # Done
        kvmsg.store(kvmap)

    # Now apply pending updates, discard out-of-sequence messages
    while True:
        try:
            kvmsg = KVMsg.recv(subscriber)
        except:
            break   # Interrupted
        if kvmsg.sequence > sequence:
            sequence = kvmsg.sequence
            kvmsg.store(kvmap)


if __name__ == '__main__':
    main()
