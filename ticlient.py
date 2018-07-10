import sys
import time

from mdcliapi import MajorDomoClient


def service_call(session, service, request):
    """Calls a TSP service

    Returns reponse if successful (status code 200 OK), else None
    """
    reply = session.send(service, request)
    if reply:
        status = reply.pop(0)
        if status == b"200":
            return reply
        elif status == b"400":
            print("E: client fatal error, aborting")
            sys.exit(1)
        elif status == b"500":
            print("E: server fatal error, aborting")
            sys.exit(1)
    else:
        sys.exit(0)   # Interrupted or failed


def main():
    verbose = '-v' in sys.argv
    session = MajorDomoClient("tcp://localhost:5555", verbose)

    service = b'echo'
    for arg in sys.argv:
        if arg.startswith('-s='):
            service = arg.split('=')[1].encode()

    msg = b'Hello world'
    for arg in sys.argv:
            if arg.startswith('-r='):
                msg = arg.split('=')[1].encode()

    # 1. Send 'echo' request to Titanic
    request = [service, msg]
    reply = service_call(session, b"titanic.request", request)

    uuid = None

    if reply:
        uuid = reply.pop(0)
        print(f"I: request UUID {uuid.decode()}")

    # 2. Wait until we get a reply
    while True:
        time.sleep(.1)
        request = [uuid]
        reply = service_call(session, b"titanic.reply", request)

        if reply:
            reply_string = reply[-1]
            print(f"Reply: {reply_string.decode()}")

            # 3. Close request
            request = [uuid]
            reply = service_call(session, b"titanic.close", request)
            break
        else:
            print("I: no reply yet, trying again…")
            time.sleep(5)  # Try again in 5 seconds
    return 0


if __name__ == '__main__':
    main()
