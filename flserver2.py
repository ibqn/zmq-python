import sys
import zmq


if len(sys.argv) != 2:
    print(f"I: Syntax: {sys.argv[0]} <endpoint>")
    sys.exit(1)

endpoint = sys.argv[1]
context = zmq.Context()
server = context.socket(zmq.REP)
server.bind(endpoint)

print(f"I: Service is ready at {endpoint}")
while True:
    request = server.recv_multipart()
    if not request:
        break  # Interrupted
    # Fail nastily if run against wrong client
    assert len(request) == 2

    address = request[0]
    reply = [address, b"OK"]
    server.send_multipart(reply)

server.setsockopt(zmq.LINGER, 0)  # Terminate early
