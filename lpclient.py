import zmq
import sys


REQUEST_TIMEOUT = 2500
REQUEST_RETRIES = 17
SERVER_ENDPOINT = "tcp://localhost:5555"


if len(sys.argv) == 2:
    myself = sys.argv[1]
else:
    print("Usage: lpclient.py <myself>")
    sys.exit(1)

context = zmq.Context(1)

print("I: Connecting to server…")
client = context.socket(zmq.REQ)
client.connect(SERVER_ENDPOINT)

poll = zmq.Poller()
poll.register(client, zmq.POLLIN)

sequence = 0
retries_left = REQUEST_RETRIES
while retries_left:
    sequence += 1
    request = [myself.encode(), str(sequence).encode()]
    print("I: Sending ({0[0]}-{0[1]})".format(
        [msg.decode() for msg in request]
    ))
    client.send_multipart(request)

    expect_reply = True
    while expect_reply:
        socks = dict(poll.poll(REQUEST_TIMEOUT))
        if socks.get(client) == zmq.POLLIN:
            [name, reply] = client.recv_multipart()
            if not reply:
                break
            if int(reply) == sequence:
                print(f"I: Server replied OK ({reply.decode()})")
                retries_left = REQUEST_RETRIES
                expect_reply = False
            else:
                print(f"E: Malformed reply from server: {reply.decode()}")

        else:
            print("W: No response in time from server, retrying…")
            # Socket is confused. Close and remove it.
            client.setsockopt(zmq.LINGER, 0)
            client.close()
            poll.unregister(client)
            retries_left -= 1
            if retries_left == 0:
                print("E: Server seems to be offline, abandoning")
                break
            print((
                "I: Reconnecting and resending "
                "{0[0]}-{0[1]} (attempt #{1})"
            ).format(
                [msg.decode() for msg in request],
                REQUEST_RETRIES - retries_left,
            ))
            # Create new connection
            client = context.socket(zmq.REQ)
            client.connect(SERVER_ENDPOINT)
            poll.register(client, zmq.POLLIN)
            client.send_multipart(request)

context.term()
