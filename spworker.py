from random import randint
import time
import zmq


LRU_READY = b"\x01"

context = zmq.Context(1)
worker = context.socket(zmq.REQ)

identity = f"{randint(0, 0x10000):04X}-{randint(0, 0x10000):04X}"
worker.setsockopt(zmq.IDENTITY, identity.encode())
worker.connect("tcp://localhost:5556")

print(f"I: ({identity}) worker ready")
worker.send(LRU_READY)

cycles = 0
while True:
    msg = worker.recv_multipart()
    if not msg:
        break

    cycles += 1
    if cycles > 3 and randint(0, 20) == 0:
        print(f"I: ({identity}) simulating a crash")
        break
    elif cycles > 3 and randint(0, 5) == 0:
        print(f"I: ({identity}) simulating CPU overload")
        time.sleep(3)
    print(
        "I: ({0}) normal reply to ({1[0]}-{1[1]})".format(
            identity,
            [c.decode() for c in msg[2:]]
        )
    )
    time.sleep(1)  # Do some heavy work
    worker.send_multipart(msg)
