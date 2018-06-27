import multiprocessing

import zmq
import zhelpers


NBR_CLIENTS = 10**2
NBR_WORKERS = 3


def client_task(ident):
    """Basic request-reply client using REQ socket."""
    socket = zmq.Context().socket(zmq.REQ)
    socket.identity = f"Client-{ident}".encode()
    socket.connect("ipc://frontend.ipc")

    # Send request, get reply
    print(f"{socket.identity.decode()}: sending 'hello'")
    socket.send(f"HELLO from client {ident}".encode())
    reply = socket.recv()
    print(f"{socket.identity.decode()}: {reply.decode()}")


def worker_task(ident):
    """Worker task, using a REQ socket to do load-balancing."""
    socket = zmq.Context().socket(zmq.REQ)
    socket.identity = f"Worker-{ident}".encode()
    socket.connect("ipc://backend.ipc")

    print(f"{socket.identity.decode()}: ready")
    # Tell broker we're ready for work
    socket.send(b"READY")

    while True:
        [address, empty, request] = socket.recv_multipart()
        print(f"{socket.identity.decode()}: {request.decode()}")
        socket.send_multipart([address, b"", b"OK"])


def main():
    """Load balancer main loop."""
    # Prepare context and sockets
    context = zmq.Context.instance()
    frontend = context.socket(zmq.ROUTER)
    frontend.bind("ipc://frontend.ipc")
    backend = context.socket(zmq.ROUTER)
    backend.bind("ipc://backend.ipc")

    # Start background tasks
    def start(task, *args):
        process = multiprocessing.Process(target=task, args=args)
        process.daemon = True
        process.start()

    for i in range(NBR_CLIENTS):
        start(client_task, i)
    for i in range(NBR_WORKERS):
        start(worker_task, i)

    # Initialize main loop state
    count = NBR_CLIENTS
    workers = []
    poller = zmq.Poller()
    # Only poll for requests from backend until workers are available
    poller.register(backend, zmq.POLLIN)

    while True:
        sockets = dict(poller.poll())

        if backend in sockets:
            # Handle worker activity on the backend
            request = backend.recv_multipart()
            zhelpers.dump(request)
            [worker, empty, client] = request[:3]
            if not workers:
                # Poll for clients now that a worker is available
                poller.register(frontend, zmq.POLLIN)
            workers.append(worker)
            if client != b"READY" and len(request) > 3:
                # If client reply, send rest back to frontend
                [empty, reply] = request[3:]
                frontend.send_multipart([client, b"", reply])
                count -= 1
                if not count:
                    break

        if frontend in sockets:
            # Get next client request, route to last-used worker
            multipart = frontend.recv_multipart()
            zhelpers.dump(multipart)
            [client, empty, request] = multipart[:3]
            worker = workers.pop(0)
            backend.send_multipart([worker, b"", client, b"", request])
            if not workers:
                # Don't poll clients if no workers are available
                poller.unregister(frontend)

    # Clean up
    backend.close()
    frontend.close()
    context.term()


if __name__ == "__main__":
    main()
