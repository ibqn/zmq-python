import random
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
    publisher = ctx.socket(zmq.PUSH)
    publisher.linger = 0
    publisher.connect("tcp://localhost:5558")

    random.seed(time.time())
    kvmap = {}

    # Get state snapshot
    sequence = 0
    snapshot.send(b"ICANHAZ?")
    while True:
        try:
            kvmsg = KVMsg.recv(snapshot)
        except:
            return  # Interrupted

        if kvmsg.key == b"KTHXBAI":
            sequence = kvmsg.sequence
            print(f"I: Received snapshot={sequence:d}")
            break   # Done
        kvmsg.store(kvmap)

    poller = zmq.Poller()
    poller.register(subscriber, zmq.POLLIN)

    alarm = time.time() + 1
    while True:
        tickless = 1000 * max(0, alarm - time.time())
        try:
            items = dict(poller.poll(tickless))
        except:
            break  # Interrupted

        if subscriber in items:
            kvmsg = KVMsg.recv(subscriber)

            # Discard out-of-sequence kvmsgs, incl. heartbeats
            if kvmsg.sequence > sequence:
                sequence = kvmsg.sequence
                kvmsg.store(kvmap)
                print(f"I: received update={sequence:d}")

        # If we timed-out, generate a random kvmsg
        if time.time() >= alarm:
            kvmsg = KVMsg(0)
            kvmsg.key = f"{random.randint(1, 10000):d}".encode()
            kvmsg.body = f"{random.randint(1, 1000000):d}".encode()
            kvmsg.send(publisher)
            kvmsg.store(kvmap)
            alarm = time.time() + 1

    print(f"Interrupted\n{sequence:d} messages in")


if __name__ == '__main__':
    main()
