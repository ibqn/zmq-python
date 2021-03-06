import sys
import time

from random import randint

import zmq

#
# Pathological publisher
# Sends out 1,000 topics and then one random update per second
#


def main(url=None):
    ctx = zmq.Context.instance()
    publisher = ctx.socket(zmq.PUB)
    if url:
        publisher.bind(url)
    else:
        publisher.bind("tcp://*:5556")
    # Ensure subscriber connection has time to complete
    time.sleep(1)

    # Send out all 1,000 topic messages
    for topic_nbr in range(1000):
        publisher.send_multipart([
            f"{topic_nbr:03d}".encode(),
            b"Save Roger",
        ])

    while True:
        # Send one random update per second
        try:
            time.sleep(1)
            publisher.send_multipart([
                f"{randint(0, 999):03d}".encode(),
                b"Off with his head!",
            ])
        except KeyboardInterrupt:
            print("interrupted")
            break


if __name__ == '__main__':
    main(sys.argv[1] if len(sys.argv) > 1 else None)
