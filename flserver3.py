import sys

import zmq

from zhelpers import dump


def main():
    verbose = '-v' in sys.argv

    ctx = zmq.Context()
    # Prepare server socket with predictable identity
    bind_endpoint = "tcp://*:5555"
    connect_endpoint = "tcp://localhost:5555"
    server = ctx.socket(zmq.ROUTER)
    server.identity = connect_endpoint.encode()
    server.bind(bind_endpoint)
    print(f"I: service is ready at {bind_endpoint}")

    while True:
        try:
            request = server.recv_multipart()
        except:
            break  # Interrupted
        # Frame 0: identity of client
        # Frame 1: PING, or client control frame
        # Frame 2: request body
        [address, control] = request[:2]
        reply = [address, control]
        if control == b"PING":
            reply[1] = b"PONG"
        else:
            reply.append(b"OK")
        if verbose:
            dump(reply)
        server.send_multipart(reply)
    print("W: interrupted")


if __name__ == '__main__':
    main()
