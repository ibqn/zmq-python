import sys
import threading
import time

import zmq

from zhelpers import zpipe


def client_task(ctx, pipe):
    client = ctx.socket(zmq.DEALER)
    client.identity = b'C'
    client.connect("tcp://localhost:5555")

    print("Setting up test…")
    time.sleep(.5)

    print("Synchronous round-trip test…")
    start = time.time()
    requests = 50000
    for r in range(requests):
        client.send_multipart([b"", b"hello"])
        client.recv_multipart()
    print(f" {requests / (time.time() - start):.0f} calls/second")

    print("Asynchronous round-trip test…")
    start = time.time()
    for r in range(requests):
        client.send_multipart([b"", b"hello"])
    for r in range(requests):
        client.recv_multipart()
    print(f" {requests / (time.time() - start):.0f} calls/second")

    # signal done:
    pipe.send(b"done")


def worker_task():
    ctx = zmq.Context()
    worker = ctx.socket(zmq.DEALER)
    worker.identity = b'W'
    worker.connect("tcp://localhost:5556")

    while True:
        msg = worker.recv_multipart()
        worker.send_multipart(msg)
    ctx.destroy(0)


def broker_task():
    # Prepare our context and sockets
    ctx = zmq.Context()
    frontend = ctx.socket(zmq.ROUTER)
    backend = ctx.socket(zmq.ROUTER)
    frontend.bind("tcp://*:5555")
    backend.bind("tcp://*:5556")

    # Initialize poll set
    poller = zmq.Poller()
    poller.register(backend, zmq.POLLIN)
    poller.register(frontend, zmq.POLLIN)

    while True:
        try:
            items = dict(poller.poll())
        except:
            break  # Interrupted

        if frontend in items:
            msg = frontend.recv_multipart()
            # print(f"f {msg}")
            msg[0] = b'W'
            backend.send_multipart(msg)
        if backend in items:
            msg = backend.recv_multipart()
            # print(f"b {msg}")
            msg[0] = b'C'
            frontend.send_multipart(msg)


def main():
    # Create threads
    ctx = zmq.Context()
    client, pipe = zpipe(ctx)

    client_thread = threading.Thread(
        target=client_task,
        args=(ctx, pipe)
    )
    worker_thread = threading.Thread(target=worker_task)
    worker_thread.daemon = True
    broker_thread = threading.Thread(target=broker_task)
    broker_thread.daemon = True

    worker_thread.start()
    broker_thread.start()
    client_thread.start()

    # Wait for signal on client pipe
    client.recv()


if __name__ == '__main__':
    main()
