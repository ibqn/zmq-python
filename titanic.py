import pickle
import os
import sys
import threading
import time
from uuid import uuid4
from pathlib import Path

import zmq

from mdwrkapi import MajorDomoWorker
from mdcliapi import MajorDomoClient

from zhelpers import zpipe


TITANIC_DIR = Path(".titanic")


def request_filename(uuid):
    """Returns freshly allocated request filename for given UUID"""
    return TITANIC_DIR.joinpath(f"{uuid}.req")


def reply_filename(uuid):
    """Returns freshly allocated reply filename for given UUID"""
    return TITANIC_DIR.joinpath(f"{uuid}.rep")

# ---------------------------------------------------------------------
# Titanic request service


def titanic_request(pipe, verbose=False):
    worker = MajorDomoWorker(
        "tcp://localhost:5555",
        b"titanic.request",
        verbose
    )

    reply = None

    # Ensure message directory exists
    TITANIC_DIR.mkdir(parents=True, exist_ok=True)

    while True:
        # Send reply if it's not null
        # And then get next request from broker
        request = worker.recv(reply)
        if not request:
            break  # Interrupted, exit

        # Generate UUID and save message to disk
        uuid = uuid4().hex
        filename = request_filename(uuid)
        with open(filename, 'wb') as f:
            pickle.dump(request, f)

        # Send UUID through to message queue
        pipe.send(uuid.encode())

        # Now send UUID back to client
        # Done by the worker.recv() at the top of the loop
        reply = [b"200", uuid.encode()]

# ---------------------------------------------------------------------
# Titanic reply service


def titanic_reply(verbose=False):
    worker = MajorDomoWorker(
        "tcp://localhost:5555",
        b"titanic.reply",
        verbose
    )
    reply = None

    while True:
        request = worker.recv(reply)
        if not request:
            break  # Interrupted, exit

        uuid = request.pop(0).decode()
        req_filename = request_filename(uuid)
        rep_filename = reply_filename(uuid)
        if rep_filename.exists():
            with open(rep_filename, 'rb') as f:
                reply = pickle.load(f)

            reply = [b"200"] + reply
        else:
            if req_filename.exists():
                reply = [b"300"]  # pending
            else:
                reply = [b"400"]  # unknown

# ---------------------------------------------------------------------
# Titanic close service


def titanic_close(verbose=False):
    worker = MajorDomoWorker(
        "tcp://localhost:5555",
        b"titanic.close",
        verbose
    )
    reply = None

    while True:
        request = worker.recv(reply)
        if not request:
            break  # Interrupted, exit

        uuid = request.pop(0).decode()
        req_filename = request_filename(uuid)
        rep_filename = reply_filename(uuid)
        # should these be protected?  Does zfile_delete ignore files
        # that have already been removed?  That's what we are doing here.
        if req_filename.exists():
            req_filename.unlink()
        if rep_filename.exists():
            rep_filename.unlink()
        reply = [b"200"]


def service_success(client, uuid):
    """Attempt to process a single request, return True if successful"""
    # Load request message, service will be first frame
    filename = request_filename(uuid)

    # If the client already closed request, treat as successful
    if not filename.exists():
        return True

    with open(filename, 'rb') as f:
        request = pickle.load(f)
    service = request.pop(0)
    # Use MMI protocol to check if service is available
    mmi_request = [service]
    mmi_reply = client.send(b"mmi.service", mmi_request)
    service_ok = mmi_reply and mmi_reply[0] == b"200"

    if service_ok:
        reply = client.send(service, request)
        if reply:
            filename = reply_filename(uuid)
            with open(filename, "wb") as f:
                pickle.dump(reply, f)
            return True

    return False


def main():
    verbose = '-v' in sys.argv
    ctx = zmq.Context()

    # Create MDP client session with short timeout
    # this client is used by service_success method
    client = MajorDomoClient("tcp://localhost:5555", verbose)
    client.timeout = 1000  # 1 sec
    client.retries = 1  # only 1 retry

    request_pipe, peer = zpipe(ctx)
    request_thread = threading.Thread(
        target=titanic_request,
        args=(peer, verbose, )
    )
    request_thread.daemon = True
    request_thread.start()
    reply_thread = threading.Thread(target=titanic_reply, args=(verbose, ))
    reply_thread.daemon = True
    reply_thread.start()
    close_thread = threading.Thread(target=titanic_close, args=(verbose, ))
    close_thread.daemon = True
    close_thread.start()

    poller = zmq.Poller()
    poller.register(request_pipe, zmq.POLLIN)

    # Ensure message directory exists
    TITANIC_DIR.mkdir(parents=True, exist_ok=True)
    # create the dispatcher queue file, if not present
    queue = TITANIC_DIR.joinpath('queue')
    queue.touch()

    # Main dispatcher loop
    while True:
        # We'll dispatch once per second, if there's no activity
        try:
            items = poller.poll(1000)
        except KeyboardInterrupt:
            break  # Interrupted

        if items:
            # Append UUID to queue, prefixed with '-' for pending
            uuid = request_pipe.recv()
            with open(queue, 'a') as f:
                f.write(f"-{uuid.decode()}\n")

        # Brute-force dispatcher
        with open(queue, 'r+b') as f:
            for entry in f.readlines():
                entry = entry.decode()
                # UUID is prefixed with '-' if still waiting
                if entry[0] == '-':
                    uuid = entry[1:].rstrip()  # rstrip '\n' etc.
                    print(f"I: processing request {uuid}")
                    if service_success(client, uuid):
                        # mark queue entry as processed
                        here = f.tell()
                        f.seek(-1 * len(entry), os.SEEK_CUR)
                        f.write(b'+')
                        f.seek(here, os.SEEK_SET)
                        print(f"completed {uuid}")


if __name__ == '__main__':
    main()
