import sys
from mdwrkapi import MajorDomoWorker


def main():
    verbose = '-v' in sys.argv
    worker = MajorDomoWorker("tcp://localhost:5555", "echo", verbose)
    reply = None
    while True:
        request = worker.recv(reply)
        if request is None:
            break  # Worker was interrupted
        reply = request  # Echo is complex… :-)


if __name__ == '__main__':
    main()
