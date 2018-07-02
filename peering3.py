import random
import sys
import threading
import time

import zmq

NBR_CLIENTS = 10
NBR_WORKERS = 5


def asbytes(obj):
    return str(obj).encode()


def client_task(name, i):
    """Request-reply client using REQ socket"""
    ctx = zmq.Context()
    client = ctx.socket(zmq.REQ)
    client.identity = f"Client-{name}-{i}".encode()
    client.connect(f"ipc://{name}-localfe.ipc")
    monitor = ctx.socket(zmq.PUSH)
    monitor.connect(f"ipc://{name}-monitor.ipc")

    poller = zmq.Poller()
    poller.register(client, zmq.POLLIN)
    while True:
        time.sleep(random.randint(0, 5))
        for _ in range(random.randint(0, 15)):
            # send request with random hex ID
            task_id = f"{random.randint(0, 10000):04X}"
            client.send_string(task_id)

            # wait max 10 seconds for a reply, then complain
            try:
                events = dict(poller.poll(10000))
            except zmq.ZMQError:
                return  # interrupted

            if events:
                reply = client.recv_string()
                assert reply == task_id, f"expected {task_id}, got {reply}"
                monitor.send_string(reply)
            else:
                monitor.send_string(f"E: CLIENT EXIT - lost task {task_id}")
                return


def worker_task(name, i):
    """Worker using REQ socket to do LRU routing"""
    ctx = zmq.Context()
    worker = ctx.socket(zmq.REQ)
    worker.identity = f"Worker-{name}-{i}".encode()
    worker.connect(f"ipc://{name}-localbe.ipc")

    # Tell broker we're ready for work
    worker.send(b"READY")

    # Process messages as they arrive
    while True:
        try:
            msg = worker.recv_multipart()
        except zmq.ZMQError:
            return  # interrupted

        # Workers are busy for 0/1 seconds
        time.sleep(random.randint(0, 1))
        worker.send_multipart(msg)


def main(myself, peers):
    print(f"I: preparing broker at {myself}…")

    # Prepare our context and sockets
    ctx = zmq.Context()

    # Bind cloud frontend to endpoint
    cloudfe = ctx.socket(zmq.ROUTER)
    cloudfe.setsockopt(zmq.IDENTITY, myself)
    cloudfe.bind(f"ipc://{myself}-cloud.ipc")

    # Bind state backend / publisher to endpoint
    statebe = ctx.socket(zmq.PUB)
    statebe.bind(f"ipc://{myself}-state.ipc")

    # Connect cloud and state backends to all peers
    cloudbe = ctx.socket(zmq.ROUTER)
    statefe = ctx.socket(zmq.SUB)
    statefe.setsockopt(zmq.SUBSCRIBE, b"")
    cloudbe.setsockopt(zmq.IDENTITY, myself)

    for peer in peers:
        print(f"I: connecting to cloud frontend at {peer}")
        cloudbe.connect(f"ipc://{peer}-cloud.ipc")
        print(f"I: connecting to state backend at {peer}")
        statefe.connect(f"ipc://{peer}-state.ipc")

    # Prepare local frontend and backend
    localfe = ctx.socket(zmq.ROUTER)
    localfe.bind(f"ipc://{myself}-localfe.ipc")
    localbe = ctx.socket(zmq.ROUTER)
    localbe.bind(f"ipc://{myself}-localbe.ipc")

    # Prepare monitor socket
    monitor = ctx.socket(zmq.PULL)
    monitor.bind(f"ipc://{myself}-monitor.ipc")

    # Get user to tell us when we can start…
    input("Press Enter when all brokers are started: ")

    # create workers and clients threads
    for i in range(NBR_WORKERS):
        thread = threading.Thread(target=worker_task, args=(myself, i))
        thread.daemon = True
        thread.start()

    for i in range(NBR_CLIENTS):
        thread_c = threading.Thread(target=client_task, args=(myself, i))
        thread_c.daemon = True
        thread_c.start()

    # Interesting part
    # -------------------------------------------------------------
    # Publish-subscribe flow
    # - Poll statefe and process capacity updates
    # - Each time capacity changes, broadcast new value
    # Request-reply flow
    # - Poll primary and process local/cloud replies
    # - While worker available, route localfe to local or cloud

    local_capacity = 0
    cloud_capacity = 0
    workers = []

    # setup backend poller
    pollerbe = zmq.Poller()
    pollerbe.register(localbe, zmq.POLLIN)
    pollerbe.register(cloudbe, zmq.POLLIN)
    pollerbe.register(statefe, zmq.POLLIN)
    pollerbe.register(monitor, zmq.POLLIN)

    while True:
        # If we have no workers anyhow, wait indefinitely
        try:
            events = dict(pollerbe.poll(1000 if local_capacity else None))
        except zmq.ZMQError:
            break  # interrupted

        previous = local_capacity
        # Handle reply from local worker
        msg = None
        if localbe in events:
            msg = localbe.recv_multipart()
            [address, empty], msg = msg[:2], msg[2:]
            workers.append(address)
            local_capacity += 1

            # If it's READY, don't route the message any further
            if msg[-1] == b'READY':
                msg = None
        elif cloudbe in events:
            msg = cloudbe.recv_multipart()
            [address, empty], msg = msg[:2], msg[2:]

            # We don't use peer broker address for anything

        if msg is not None:
            address = msg[0]
            if address in peers:
                # Route reply to cloud if it's addressed to a broker
                cloudfe.send_multipart(msg)
            else:
                # Route reply to client if we still need to
                localfe.send_multipart(msg)

        # Handle capacity updates
        if statefe in events:
            [peer, s] = statefe.recv_multipart()
            cloud_capacity = int(s)

        # handle monitor message
        if monitor in events:
            print(monitor.recv_string())

        # Now route as many clients requests as we can handle
        # - If we have local capacity we poll both localfe and cloudfe
        # - If we have cloud capacity only, we poll just localfe
        # - Route any request locally if we can, else to cloud
        while local_capacity + cloud_capacity:
            secondary = zmq.Poller()
            secondary.register(localfe, zmq.POLLIN)
            if local_capacity:
                secondary.register(cloudfe, zmq.POLLIN)
            events = dict(secondary.poll(0))

            # We'll do peer brokers first, to prevent starvation
            if cloudfe in events:
                msg = cloudfe.recv_multipart()
            elif localfe in events:
                msg = localfe.recv_multipart()
            else:
                break  # No work, go back to backends

            if local_capacity:
                msg = [workers.pop(0), b''] + msg
                localbe.send_multipart(msg)
                local_capacity -= 1
            else:
                # Route to random broker peer
                msg = [random.choice(peers), b''] + msg
                cloudbe.send_multipart(msg)
        if local_capacity != previous:
            statebe.send_multipart([myself, asbytes(local_capacity)])


if __name__ == '__main__':
    if len(sys.argv) >= 2:
        myself = asbytes(sys.argv[1])
        main(myself, peers=[asbytes(a) for a in sys.argv[2:]])
    else:
        print("Usage: peering3.py <myself> <peer_1> … <peer_N>")
        sys.exit(1)
