import sys
from mdcliapi import MajorDomoClient


def main():
    verbose = '-v' in sys.argv
    request = b"echo"
    for arg in sys.argv:
        if arg.startswith('-s='):
            request = arg.split('=')[1].encode()

    client = MajorDomoClient("tcp://localhost:5555", verbose)
    reply = client.send(b"mmi.service", request)

    if reply:
        replycode = reply[0]
        print(f"Lookup echo service: {replycode.decode()}")
    else:
        print("E: no response from broker, make sure it's running")


if __name__ == '__main__':
    main()
