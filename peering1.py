import sys
import time
import random

import zmq


def main(myself, others):
    print(f"Hello, I am {myself}")

    context = zmq.Context()

    # State Back-End
    statebe = context.socket(zmq.PUB)

    # State Front-End
    statefe = context.socket(zmq.SUB)

    statefe.setsockopt(zmq.SUBSCRIBE, b'')

    bind_address = f"ipc://{myself}-state.ipc"
    statebe.bind(bind_address)

    for other in others:
        statefe.connect(f"ipc://{other}-state.ipc")
        time.sleep(1.0)

    poller = zmq.Poller()
    poller.register(statefe, zmq.POLLIN)

    while True:
        # Solution with poll()
        socks = dict(poller.poll(1000))

        # Handle incoming status message
        if socks.get(statefe) == zmq.POLLIN:
            msg = [m.decode() for m in statefe.recv_multipart()]
            print(f'{myself} Received: {msg}')

        else:
            # Send our address and a random value
            # for worker availability
            msg = [bind_address, str(random.randrange(1, 10))]
            msg = [m.encode() for m in msg]
            statebe.send_multipart(msg)

# Solution with select()
#        pollin, pollout, pollerr = zmq.select([statefe], [], [], 1)
#
#        if pollin and pollin[0] == statefe:
#            # Handle incoming status message
#            msg = statefe.recv_multipart()
#            print 'Received:', msg
#
#        else:
#            # Send our address and a random value
#            # for worker availability
#            msg = [bind_address, str(random.randrange(1, 10))]
#            statebe.send_multipart(msg)


if __name__ == '__main__':
    if len(sys.argv) >= 2:
        try:
            main(
                myself=sys.argv[1],
                others=sys.argv[2:]
            )
        except KeyboardInterrupt:
            pass
    else:
        print('Usage: peering.py <myself> <peer_1> … <peer_N>')
        sys.exit(1)
