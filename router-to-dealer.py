import time
import random
from threading import Thread

import zmq


# We have two workers, here we copy the code, normally these would
# run on different boxes…
def worker_name(name):
    def worker(context=None):
        context = context or zmq.Context.instance()
        worker = context.socket(zmq.DEALER)
        worker.setsockopt(zmq.IDENTITY, name.encode())
        worker.connect("ipc://routing.ipc")

        total = 0
        while True:
            # We receive one part, with the workload
            request = worker.recv()
            finished = request == b"END"
            if finished:
                print(f"{name} received: {total}")
                break
            total += 1

    return worker


context = zmq.Context.instance()
client = context.socket(zmq.ROUTER)
client.bind("ipc://routing.ipc")

Thread(target=worker_name('A')).start()
Thread(target=worker_name('B')).start()

# Wait for threads to stabilize
time.sleep(1)

# Send 10 tasks scattered to A twice as often as B
for _ in range(100):
    # Send two message parts, first the address…
    ident = random.choice([b'A', b'A', b'B'])
    # And then the workload
    work = b"This is the workload"
    client.send_multipart([ident, work])

client.send_multipart([b'A', b'END'])
client.send_multipart([b'B', b'END'])
