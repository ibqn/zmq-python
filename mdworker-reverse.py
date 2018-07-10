import sys
from mdwrkapi import MajorDomoWorker


def main():
    verbose = '-v' in sys.argv
    worker = MajorDomoWorker("tcp://localhost:5555", b"reverse", verbose)
    reply = None
    while True:
        request = worker.recv(reply)
        if request is None:
            break  # Worker was interrupted
        # reverse the content of the message in form of [msg]
        request = [request[0].decode()[::-1].encode()]
        reply = request  # Reverse is complex… :-)


if __name__ == '__main__':
    main()
