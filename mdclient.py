import sys
from mdcliapi import MajorDomoClient


def main():
    verbose = '-v' in sys.argv
    client = MajorDomoClient("tcp://localhost:5555", verbose)
    count = 0
    while count < 100000:
        request = "Hello world"
        try:
            reply = client.send("echo", request)
        except KeyboardInterrupt:
            break
        else:
            # also break on failure to reply:
            if reply is None:
                break
        count += 1
    print(f"{count} requests/replies processed")


if __name__ == '__main__':
    main()
