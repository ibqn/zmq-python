import zmq
import zhelpers


context = zmq.Context()

sink = context.socket(zmq.ROUTER)
sink.bind("inproc://example")

# First allow 0MQ to set the identity
anonymous = context.socket(zmq.DEALER)
anonymous.connect("inproc://example")
anonymous.send(b"ROUTER uses a generated 5 byte identity")
zhelpers.dump(sink)

# Then set the identity ourselves
identified = context.socket(zmq.DEALER)
identified.setsockopt(zmq.IDENTITY, b"PEER2")
identified.connect("inproc://example")
# DEALER socket does not prepend the zero frame, thus
# we add it manually to simulate REQ socket
identified.send_multipart([
    b"",
    b"ROUTER socket uses DEALER's socket identity"
])
zhelpers.dump(sink)

# Then set the identity ourselves on the REQ socket
req = context.socket(zmq.REQ)
req.setsockopt(zmq.IDENTITY, b"PEER3")
req.connect("inproc://example")
req.send(b"ROUTER socket uses REQ's socket identity")
zhelpers.dump(sink)
