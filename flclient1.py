import sys
import time

import zmq


REQUEST_TIMEOUT = 1000  # ms
MAX_RETRIES = 3   # Before we abandon


def try_request(ctx, endpoint, request):
    print(f"I: Trying echo service at {endpoint}…")
    client = ctx.socket(zmq.REQ)
    client.setsockopt(zmq.LINGER, 0)  # Terminate early
    client.connect(endpoint)
    client.send(request)
    poll = zmq.Poller()
    poll.register(client, zmq.POLLIN)
    socks = dict(poll.poll(REQUEST_TIMEOUT))
    if socks.get(client) == zmq.POLLIN:
        reply = client.recv_multipart()
    else:
        reply = None
    poll.unregister(client)
    client.close()
    return reply

context = zmq.Context()
request = b"Hello world"
reply = None

endpoints = len(sys.argv) - 1
if endpoints == 0:
    print(f"I: syntax: {sys.argv[0]} <endpoint> …")
elif endpoints == 1:
    # For one endpoint, we retry N times
    endpoint = sys.argv[1]
    for retries in range(MAX_RETRIES):
        reply = try_request(context, endpoint, request)
        if reply:
            break  # Success
        print(f"W: No response from {endpoint}, retrying")
else:
    # For multiple endpoints, try each at most once
    for endpoint in sys.argv[1:]:
        reply = try_request(context, endpoint, request)
        if reply:
            break  # Success
        print(f"W: No response from {endpoint}")

if reply:
    print("Service is running OK")
