import sys
from mdcliapi2 import MajorDomoClient


def main():
    verbose = '-v' in sys.argv
    client = MajorDomoClient("tcp://localhost:5555", verbose)

    service = b'echo'
    for arg in sys.argv:
        if arg.startswith('-s='):
            service = arg.split('=')[1].encode()

    request = b'Hello world'
    for arg in sys.argv:
        if arg.startswith('-r='):
            request = arg.split('=')[1].encode()

    try:
        client.send(service, request)
    except KeyboardInterrupt:
        print("send interrupted, aborting")
        sys.exit(1)

    try:
        reply = client.recv()
    except KeyboardInterrupt:
        print("receive interrupted, aborting")
        sys.exit(1)
    # also break on failure to reply:
    if reply is None:
        print("no answer receiven, aborting")
        sys.exit(1)

    print(
        f"received answer '{reply[0].decode()}' "
        f"for the request '{request.decode()}'"
    )


if __name__ == '__main__':
    main()
