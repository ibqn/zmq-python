import struct  # for packing integers
import sys

import zmq


class KVMsg(object):
    """
    Message is formatted on wire as 3 frames:
    frame 0: key (0MQ string)
    frame 1: sequence (8 bytes, network order)
    frame 2: body (blob)
    """
    key = None  # key (string)
    sequence = 0  # int
    body = None  # blob

    def __init__(self, sequence, key=None, body=None):
        assert isinstance(sequence, int)
        self.sequence = sequence
        self.key = key
        self.body = body

    def store(self, dikt):
        """Store me in a dict if I have anything to store"""
        # this seems weird to check, but it's what the C example does
        if self.key is not None and self.body is not None:
            dikt[self.key] = self

    def send(self, socket):
        """
        Send key-value message to socket;
        any empty frames are sent as such.
        """
        key = b'' if self.key is None else self.key
        seq_s = struct.pack('!l', self.sequence)
        body = b'' if self.body is None else self.body
        socket.send_multipart([key, seq_s, body])

    @classmethod
    def recv(cls, socket):
        """Reads key-value message from socket, returns new kvmsg instance."""
        [key, seq_s, body] = socket.recv_multipart()
        key = key if key else None
        seq = struct.unpack('!l', seq_s)[0]
        body = body if body else None
        return cls(seq, key=key, body=body)

    def dump(self):
        if self.body is None:
            size = 0
            data = 'NULL'
        else:
            size = len(self.body)
            data = self.body.decode()
        print(
            f"[seq: {self.sequence}][key: {self.key.decode()}]"
            f"[size:{size}] {data}"
        )

# ---------------------------------------------------------------------
# Runs self test of class


def test_kvmsg(verbose):
    print(" * kvmsg: ")

    # Prepare our context and sockets
    ctx = zmq.Context()
    pub = ctx.socket(zmq.DEALER)
    pub.bind("ipc://kvmsg_selftest.ipc")

    sub = ctx.socket(zmq.DEALER)
    sub.connect("ipc://kvmsg_selftest.ipc")

    kvmap = {}
    # Test send and receive of simple message
    kvmsg = KVMsg(1)
    kvmsg.key = b"parameter"
    kvmsg.body = b"content of the body"
    if verbose:
        print('send as publisher')
        kvmsg.dump()
    kvmsg.send(pub)
    kvmsg.store(kvmap)

    kvmsg2 = KVMsg.recv(sub)
    if verbose:
        print('receive as subscriber')
        kvmsg2.dump()
    assert kvmsg2.key == b"parameter"
    kvmsg2.store(kvmap)

    assert len(kvmap) == 1  # shouldn't be different

    print("OK")


if __name__ == '__main__':
    test_kvmsg('-v' in sys.argv)
