import zmq

from kvsimple import KVMsg


# simple struct for routing information for a key-value snapshot


class Route:
    def __init__(self, socket, identity, subtree):
        self.socket = socket        # ROUTER socket to send to
        self.identity = identity    # Identity of peer who requested state
        self.subtree = subtree      # Client subtree specification


def send_single(key, kvmsg, route):
    """Send one state snapshot key-value pair to a socket"""
    # check front of key against subscription subtree:
    if kvmsg.key.startswith(route.subtree):
        # Send identity of recipient first
        route.socket.send(route.identity, zmq.SNDMORE)
        kvmsg.send(route.socket)


def main():
    # context and sockets
    ctx = zmq.Context()
    snapshot = ctx.socket(zmq.ROUTER)
    snapshot.bind("tcp://*:5556")
    publisher = ctx.socket(zmq.PUB)
    publisher.bind("tcp://*:5557")
    collector = ctx.socket(zmq.PULL)
    collector.bind("tcp://*:5558")

    sequence = 0
    kvmap = {}

    poller = zmq.Poller()
    poller.register(collector, zmq.POLLIN)
    poller.register(snapshot, zmq.POLLIN)
    while True:
        try:
            items = dict(poller.poll(1000))
        except:
            break  # Interrupted

        # Apply state update sent from client
        if collector in items:
            kvmsg = KVMsg.recv(collector)
            sequence += 1
            kvmsg.sequence = sequence
            kvmsg.send(publisher)
            kvmsg.store(kvmap)
            print(f"I: publishing update {sequence:5d}")

        # Execute state snapshot request
        if snapshot in items:
            msg = snapshot.recv_multipart()
            [identity, request, subtree] = msg
            if request != b"ICANHAZ?":
                print("E: bad request, aborting")
                break

            # Send state snapshot to client
            route = Route(snapshot, identity, subtree)

            # For each entry in kvmap, send kvmsg to client
            for k, v in kvmap.items():
                send_single(k, v, route)

            # Now send END message with sequence number
            print(f"Sending state shapshot={sequence:d}")
            snapshot.send(identity, zmq.SNDMORE)
            kvmsg = KVMsg(sequence)
            kvmsg.key = b"KTHXBAI"
            kvmsg.body = subtree
            kvmsg.send(snapshot)

    print(f"Interrupted\n{sequence:d} messages handled")


if __name__ == '__main__':
    main()
