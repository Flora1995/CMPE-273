import zmq

context = zmq.Context()

dashboard_receiver = context.socket(zmq.PULL)
dashboard_receiver.connect("tcp://127.0.0.1:1235")

while True:
    message= dashboard_receiver.recv()
    message = message.decode()
    print("dashboard: ", message)