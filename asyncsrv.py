import zmq
import sys
import threading
import time
from random import (
    randint,
    random,
)
from zhelpers import tprint


class ClientTask(threading.Thread):
    """ClientTask"""
    def __init__(self, id):
        self.id = id
        threading.Thread.__init__(self)

    def run(self):
        context = zmq.Context()
        socket = context.socket(zmq.DEALER)
        identity = f'worker-{self.id}'
        socket.identity = identity.encode('ascii')
        socket.connect('tcp://localhost:5570')
        print(f'Client {identity} started')
        poll = zmq.Poller()
        poll.register(socket, zmq.POLLIN)
        reqs = 0
        while True:
            reqs += 1
            print(f'Req #{reqs} sent..')
            socket.send_string(f'request #{reqs}')
            for i in range(5):
                sockets = dict(poll.poll(1000))
                if socket in sockets:
                    msg = socket.recv()
                    tprint(f'Client {identity} received: {msg.decode()}')

        socket.close()
        context.term()


class ServerTask(threading.Thread):
    """ServerTask"""
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        context = zmq.Context()
        frontend = context.socket(zmq.ROUTER)
        frontend.bind('tcp://*:5570')

        backend = context.socket(zmq.DEALER)
        backend.bind('inproc://backend')

        workers = []
        for i in range(5):
            worker = ServerWorker(context)
            worker.start()
            workers.append(worker)

        zmq.proxy(frontend, backend)

        frontend.close()
        backend.close()
        context.term()


class ServerWorker(threading.Thread):
    """ServerWorker"""
    def __init__(self, context):
        threading.Thread.__init__(self)
        self.context = context

    def run(self):
        worker = self.context.socket(zmq.DEALER)
        worker.connect('inproc://backend')
        tprint('Worker started')
        while True:
            [ident, msg] = worker.recv_multipart()
            tprint(f'Worker received "{msg.decode()}" from {ident.decode()}')
            replies = randint(0, 4)
            for i in range(replies):
                time.sleep(1. / (randint(1, 10)))
                worker.send_multipart([ident, msg])

        worker.close()


def main():
    """main function"""
    server = ServerTask()
    server.start()
    for i in range(3):
        client = ClientTask(i)
        client.start()

    server.join()


if __name__ == "__main__":
    main()
