import sys
import zmq


if len(sys.argv) != 2:
    print(f"I: Syntax: {sys.argv[0]} <endpoint>")
    sys.exit(1)

endpoint = sys.argv[1]
context = zmq.Context()
server = context.socket(zmq.REP)
server.bind(endpoint)

print(f"I: Echo service is ready at {endpoint}")
while True:
    msg = server.recv_multipart()
    if not msg:
        break  # Interrupted
    server.send_multipart(msg)

server.setsockopt(zmq.LINGER, 0)  # Terminate immediately
