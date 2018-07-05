import logging
import sys
import time
from binascii import hexlify

import zmq

# local
import MDP
from zhelpers import dump


class Service(object):
    """a single Service"""
    name = None  # Service name
    requests = None  # List of client requests
    waiting = None  # List of waiting workers

    def __init__(self, name):
        self.name = name
        self.requests = []
        self.waiting = []


class Worker(object):
    """a Worker, idle or active"""
    identity = None  # hex Identity of worker
    address = None  # Address to route to
    service = None  # Owning service, if known
    expiry = None  # expires at this point, unless heartbeat

    def __init__(self, identity, address, lifetime):
        self.identity = identity
        self.address = address
        self.expiry = time.time() + 1e-3 * lifetime


class MajorDomoBroker(object):
    """
    Majordomo Protocol broker
    A minimal implementation of http:#rfc.zeromq.org/spec:7 and spec:8
    """

    # We'd normally pull these from config data
    INTERNAL_SERVICE_PREFIX = b"mmi."
    HEARTBEAT_LIVENESS = 3  # 3-5 is reasonable
    HEARTBEAT_INTERVAL = 2500  # msecs
    HEARTBEAT_EXPIRY = HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS

    # ---------------------------------------------------------------------

    ctx = None  # Our context
    socket = None  # Socket for clients & workers
    poller = None  # our Poller

    heartbeat_at = None  # When to send HEARTBEAT
    services = None  # known services
    workers = None  # known workers
    waiting = None  # idle workers

    verbose = False  # Print activity to stdout

    # ---------------------------------------------------------------------

    def __init__(self, verbose=False):
        """Initialize broker state."""
        self.verbose = verbose
        self.services = {}
        self.workers = {}
        self.waiting = []
        self.heartbeat_at = time.time() + 1e-3 * self.HEARTBEAT_INTERVAL
        self.ctx = zmq.Context()
        self.socket = self.ctx.socket(zmq.ROUTER)
        self.socket.linger = 0
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)
        logging.basicConfig(
            format="%(asctime)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            level=logging.INFO
        )

    # ---------------------------------------------------------------------

    def mediate(self):
        """Main broker work happens here"""
        while True:
            try:
                items = self.poller.poll(self.HEARTBEAT_INTERVAL)
            except KeyboardInterrupt:
                break  # Interrupted
            if items:
                msg = self.socket.recv_multipart()
                if self.verbose:
                    logging.info("I: received message:")
                    dump(msg, MDP.format_msg)

                sender = msg.pop(0)
                empty = msg.pop(0)
                assert empty == b''
                header = msg.pop(0)

                if (MDP.C_CLIENT == header):
                    self.process_client(sender, msg)
                elif (MDP.W_WORKER == header):
                    self.process_worker(sender, msg)
                else:
                    logging.error("E: invalid message:")
                    dump(msg)

            self.purge_workers()
            self.send_heartbeats()

    def destroy(self):
        """Disconnect all workers, destroy context."""
        while self.workers:
            self.delete_worker(self.workers.values()[0], True)
        self.ctx.destroy(0)

    def process_client(self, sender, msg):
        """Process a request coming from a client."""
        assert len(msg) >= 2  # Service name + body
        service = msg.pop(0)
        # Set reply return address to client sender
        msg = [sender, b''] + msg
        if service.startswith(self.INTERNAL_SERVICE_PREFIX):
            self.service_internal(service, msg)
        else:
            self.dispatch(self.require_service(service), msg)

    def process_worker(self, sender, msg):
        """Process message sent to us by a worker."""
        assert len(msg) >= 1  # At least, command

        command = msg.pop(0)

        worker_ready = hexlify(sender) in self.workers

        worker = self.require_worker(sender)

        if (MDP.W_READY == command):
            assert len(msg) >= 1  # At least, a service name
            service = msg.pop(0)
            # Not first command in session or Reserved service name
            if (
                worker_ready or
                service.startswith(self.INTERNAL_SERVICE_PREFIX)
            ):
                self.delete_worker(worker, True)
            else:
                # Attach worker to service and mark as idle
                worker.service = self.require_service(service)
                self.worker_waiting(worker)

        elif (MDP.W_REPLY == command):
            if (worker_ready):
                # Remove & save client return envelope and insert the
                # protocol header and service name, then rewrap envelope.
                client = msg.pop(0)
                empty = msg.pop(0)
                assert empty == b''
                msg = [client, b'', MDP.C_CLIENT, worker.service.name] + msg
                self.socket.send_multipart(msg)
                self.worker_waiting(worker)
            else:
                self.delete_worker(worker, True)

        elif (MDP.W_HEARTBEAT == command):
            if (worker_ready):
                worker.expiry = time.time() + 1e-3 * self.HEARTBEAT_EXPIRY
            else:
                self.delete_worker(worker, True)

        elif (MDP.W_DISCONNECT == command):
            self.delete_worker(worker, False)
        else:
            logging.error("E: invalid message:")
            dump(msg)

    def delete_worker(self, worker, disconnect):
        """Deletes worker from all data structures, and deletes worker."""
        assert worker is not None
        if disconnect:
            self.send_to_worker(worker, MDP.W_DISCONNECT)

        if worker.service is not None:
            worker.service.waiting.remove(worker)
        self.workers.pop(worker.identity)

    def require_worker(self, address):
        """Finds the worker (creates if necessary)."""
        assert (address is not None)
        identity = hexlify(address)
        worker = self.workers.get(identity)
        if (worker is None):
            worker = Worker(identity, address, self.HEARTBEAT_EXPIRY)
            self.workers[identity] = worker
            if self.verbose:
                logging.info(f"I: registering new worker: {identity.decode()}")

        return worker

    def require_service(self, name):
        """Locates the service (creates if necessary)."""
        assert (name is not None)
        service = self.services.get(name)
        if (service is None):
            service = Service(name)
            self.services[name] = service

        return service

    def bind(self, endpoint):
        """Bind broker to endpoint, can call this multiple times.

        We use a single socket for both clients and workers.
        """
        self.socket.bind(endpoint)
        logging.info(f"I: MDP broker/0.1.1 is active at {endpoint}")

    def service_internal(self, service, msg):
        """Handle internal service according to 8/MMI specification"""
        returncode = b"501"
        if b"mmi.service" == service:
            name = msg[-1]
            returncode = b"200" if name in self.services else b"404"
        msg[-1] = returncode

        # insert the protocol header and service name
        # after the routing envelope ([client, ''])
        msg = msg[:2] + [MDP.C_CLIENT, service] + msg[2:]
        self.socket.send_multipart(msg)

    def send_heartbeats(self):
        """Send heartbeats to idle workers if it's time"""
        if (time.time() > self.heartbeat_at):
            for worker in self.waiting:
                self.send_to_worker(worker, MDP.W_HEARTBEAT)

            self.heartbeat_at = time.time() + 1e-3 * self.HEARTBEAT_INTERVAL

    def purge_workers(self):
        """Look for and kill expired workers."""
        # we need to duplicate the sequence with [:]
        # which we modify while iterating over it
        # for index, worker in enumerate(self.waiting[:]):
        for worker in self.waiting[:]:
            if worker.expiry < time.time():
                addr = worker.identity.decode()
                logging.info(f"I: deleting expired worker: {addr}")
                self.delete_worker(worker, False)
                # we use enumerate to obtain an index
                # so we can use del to remove worker by index
                # instead of relying on self.waiting.remove(worker) which
                # performs an additional search to find an item for removing
                # del self.waiting[index]
                self.waiting.remove(worker)

    def worker_waiting(self, worker):
        """This worker is now waiting for work."""
        # Queue to broker and service waiting lists
        self.waiting.append(worker)
        worker.service.waiting.append(worker)
        worker.expiry = time.time() + 1e-3 * self.HEARTBEAT_EXPIRY
        self.dispatch(worker.service, None)

    def dispatch(self, service, msg):
        """Dispatch requests to waiting workers as possible"""
        assert (service is not None)
        if msg is not None:  # Queue message if any
            service.requests.append(msg)
        self.purge_workers()
        while service.waiting and service.requests:
            msg = service.requests.pop(0)
            worker = service.waiting.pop(0)
            self.waiting.remove(worker)
            self.send_to_worker(worker, MDP.W_REQUEST, None, msg)

    def send_to_worker(self, worker, command, option=None, msg=None):
        """Send message to worker.

        If message is provided, sends that message.
        """

        if msg is None:
            msg = []
        elif not isinstance(msg, list):
            msg = [msg]

        # Stack routing and protocol envelopes to start of message
        # and routing envelope
        if option is not None:
            msg = [option] + msg
        msg = [worker.address, b'', MDP.W_WORKER, command] + msg

        if self.verbose:
            logging.info(
                f"I: sending {MDP.format_msg(command)} "
                f"to worker {hexlify(worker.address).decode()}"
            )
            dump(msg, MDP.format_msg)

        self.socket.send_multipart(msg)


def main():
    """create and start new broker"""
    verbose = '-v' in sys.argv
    broker = MajorDomoBroker(verbose)
    broker.bind("tcp://*:5555")
    broker.mediate()


if __name__ == '__main__':
    main()
