import sys
from mdcliapi import MajorDomoClient


def main():
    verbose = '-v' in sys.argv
    client = MajorDomoClient("tcp://localhost:5555", verbose)
    request = b"echo"
    reply = client.send(b"mmi.service", request)

    if reply:
        replycode = reply[0]
        print(f"Lookup echo service: {replycode.decode()}")
    else:
        print("E: no response from broker, make sure it's running")


if __name__ == '__main__':
    main()
