import sys
import logging
from argparse import ArgumentParser
import time
from enum import IntEnum
from zhelpers import dump

import zmq


class State(IntEnum):
    PRIMARY = 1
    BACKUP = 2
    ACTIVE = 3
    PASSIVE = 4


class Event(IntEnum):
    PEER_PRIMARY = 1
    PEER_BACKUP = 2
    PEER_ACTIVE = 3
    PEER_PASSIVE = 4
    CLIENT_REQUEST = 5
    PEER_TIMEOUT = 6


# constants definition
HEARTBEAT = 1000
PEERWAITLIMIT = 3 * HEARTBEAT


class BStarState(object):
    def __init__(self, state, event, peer_expiry):
        self.state = state
        self.event = event
        self.peer_expiry = peer_expiry


class BStarException(Exception):
    pass


fsm_states = {
    State.PRIMARY: {
        Event.PEER_BACKUP: (
            "I: connected to backup (slave), ready as master", State.ACTIVE,
        ),
        Event.PEER_ACTIVE: (
            "I: connected to backup (master), ready as slave", State.PASSIVE,
        ),
    },
    State.BACKUP: {
        Event.PEER_ACTIVE: (
            "I: connected to primary (master), ready as slave", State.PASSIVE,
        ),
        Event.CLIENT_REQUEST: ("", False, ),
        Event.PEER_TIMEOUT: (
            "I: cannot connect to primary (master), ready as slave",
            State.PASSIVE,
        ),
    },
    State.ACTIVE: {
        Event.PEER_ACTIVE: (
            "E: fatal error - dual masters, aborting", False,
        ),
    },
    State.PASSIVE: {
        Event.PEER_PRIMARY: (
            "I: primary (slave) is restarting, ready as master", State.ACTIVE,
        ),
        Event.PEER_BACKUP: (
            "I: backup (slave) is restarting, ready as master", State.ACTIVE,
        ),
        Event.PEER_PASSIVE: (
            "E: fatal error - dual slaves, aborting", False,
        ),
        # Say true, check peer later
        Event.CLIENT_REQUEST: (Event.CLIENT_REQUEST, True, ),
    }
}


def run_fsm(fsm):
    # There are some transitional states we do not want to handle
    state_dict = fsm_states.get(fsm.state, {})
    res = state_dict.get(fsm.event)
    if res:
        msg, state = res
    else:
        return
    if state is False:
        raise BStarException(msg)
    elif msg == Event.CLIENT_REQUEST:
        assert fsm.peer_expiry > 0
        if int(time.time() * 1000) > fsm.peer_expiry:
            fsm.state = State.ACTIVE
        else:
            raise BStarException()
    else:
        print(msg)
        fsm.state = state


def main():
    logging.basicConfig(
        format="%(asctime)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO
    )

    parser = ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-p", "--primary", action="store_true", default=False)
    group.add_argument("-b", "--backup", action="store_true", default=False)
    parser.add_argument("-v", "--verbose", action="store_true", default=False)
    args = parser.parse_args()

    ctx = zmq.Context()
    statepub = ctx.socket(zmq.PUB)
    statesub = ctx.socket(zmq.SUB)
    statesub.setsockopt_string(zmq.SUBSCRIBE, "")
    frontend = ctx.socket(zmq.ROUTER)

    fsm = BStarState(0, 0, 0)

    if args.primary:
        print("I: Primary master, waiting for backup (slave)")
        frontend.bind("tcp://*:5001")
        statepub.bind("tcp://*:5003")
        statesub.connect("tcp://localhost:5004")
        fsm.state = State.PRIMARY
    elif args.backup:
        print("I: Backup slave, waiting for primary (master)")
        frontend.bind("tcp://*:5002")
        statepub.bind("tcp://*:5004")
        statesub.connect("tcp://localhost:5003")
        statesub.setsockopt_string(zmq.SUBSCRIBE, "")
        fsm.state = State.BACKUP
    else:
        print("Provide either -p{rimary} | -b{ackup}")
        sys.exit(1)

    send_state_at = int(time.time() * 1000 + HEARTBEAT)
    poller = zmq.Poller()
    poller.register(frontend, zmq.POLLIN)
    poller.register(statesub, zmq.POLLIN)

    while True:
        time_left = send_state_at - int(time.time() * 1000)
        if time_left < 0:
            time_left = 0
        socks = dict(poller.poll(time_left))
        if socks.get(frontend) == zmq.POLLIN:
            msg = frontend.recv_multipart()
            if args.verbose:
                logging.info('Received frontend.')
                dump(msg)
            fsm.event = Event.CLIENT_REQUEST
            try:
                run_fsm(fsm)
                frontend.send_multipart(msg)
            except BStarException:
                del msg

        if socks.get(statesub) == zmq.POLLIN:
            msg = statesub.recv()
            if args.verbose:
                logging.info('Received state.')
                dump(msg)
            fsm.event = Event(int(msg))
            del msg
            try:
                run_fsm(fsm)
                fsm.peer_expiry = int(time.time() * 1000) + (2 * HEARTBEAT)
            except BStarException:
                break
        else:
            if not fsm.peer_expiry:
                fsm.peer_expiry = int(time.time() * 1000) + PEERWAITLIMIT
            if int(time.time() * 1000) >= fsm.peer_expiry:
                fsm.event = Event.PEER_TIMEOUT
                try:
                    run_fsm(fsm)
                except BStarException:
                    pass

        if int(time.time() * 1000) >= send_state_at:
            if args.verbose:
                logging.info(f'Sending state {fsm.state:d}.')
            statepub.send(f"{fsm.state:d}".encode())
            send_state_at = int(time.time() * 1000) + HEARTBEAT


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('keybord interrupted')
