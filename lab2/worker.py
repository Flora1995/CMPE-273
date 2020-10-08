import time
import zmq
import math

while True: 
    context = zmq.Context()
    generator_receiver = context.socket(zmq.PULL)
    generator_receiver.connect("tcp://127.0.0.1:1234")
    message = generator_receiver.recv()
    message = message.decode()
    generator_receiver.close()
    if message:
        worker_sender = context.socket(zmq.PUSH)
        worker_sender.bind("tcp://127.0.0.1:1235")
        msg_to_dashboard = "the square root of {} is {}".format(message,math.sqrt(int(message)))
        worker_sender.send(msg_to_dashboard.encode())
        worker_sender.close()
