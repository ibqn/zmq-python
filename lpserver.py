from random import randint
import time
import zmq


while True:
    context = zmq.Context(1)
    server = context.socket(zmq.REP)
    server.bind("tcp://*:5555")

    cycles = 0
    while True:
        request = server.recv_multipart()
        cycles += 1

        # Simulate various problems, after a few cycles
        if cycles > 3 and randint(0, 10) == 0:
            print("I: Simulating a crash")
            break
        elif cycles > 3 and randint(0, 3) == 0:
            print("I: Simulating CPU overload")
            time.sleep(2)

        print((
            "I: Normal request ({0[0]}-{0[1]})"
        ).format(
            [msg.decode() for msg in request]
        ))
        time.sleep(1)  # Do some heavy work
        server.send_multipart(request)

    server.close()
    context.term()
    print("Restarting…")
