import zmq
import sys
import time

context = zmq.Context()

generator_sender = context.socket(zmq.PUSH)

generator_sender.bind("tcp://127.0.0.1:1234")

for num in range(10001):
    time.sleep(1)
    print("generator: ",num)
    generator_sender.send("{}".format(num).encode())

generator_sender.close()
