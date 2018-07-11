import sys
from argparse import ArgumentParser

import zmq

from bstar import BinaryStar


def echo(socket, msg):
    """Echo service"""
    socket.send_multipart(msg)


def main():
    parser = ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-p", "--primary", action="store_true", default=False)
    group.add_argument("-b", "--backup", action="store_true", default=False)
    parser.add_argument("-v", "--verbose", action="store_true", default=False)
    args = parser.parse_args()
    # Arguments can be either of:
    #     -p  primary server, at tcp://localhost:5001
    #     -b  backup server, at tcp://localhost:5002
    if args.primary:
        star = BinaryStar(True, "tcp://*:5003", "tcp://localhost:5004")
        star.register_voter("tcp://*:5001", zmq.ROUTER, echo)
    elif args.backup:
        star = BinaryStar(False, "tcp://*:5004", "tcp://localhost:5003")
        star.register_voter("tcp://*:5002", zmq.ROUTER, echo)
    else:
        print("Provide either -p{rimary} | -b{ackup}")
        sys.exit(1)

    star.start()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('keybord interrupted')
