import consul
import zmq
import sys

c = consul.Consul()
storage_dict = {}

def server(port):
    context = zmq.Context()
    consumer = context.socket(zmq.REP)
    consumer.bind(f"tcp://127.0.0.1:{port}")

    while True:
        raw = consumer.recv_json()
        op = raw['op']
        if (op == "PUT"):
            key, value = raw['key'], raw['value']
            print(f"Server_port={port}:key={key},value={value}")
            storage_dict[key] = value
        if (op == "GET"):
            

def register_server(server):
    c.agent.service.register(
        name='server-'+server['port'],
        service_id='server-'+server['port'],
        port=int(server['port'])
    )

if __name__ == "__main__":
    num_server = 1
    if len(sys.argv) > 1:
        num_server = int(sys.argv[1])
        print(f"num_server={num_server}")

    for each_server in [1,2,3,4]:
        port = "200{}".format(each_server)
        #print(f"Starting a server at:{port}...")
        current_server = {
            'server': f"tcp://127.0.0.1:{port}",
            'port': port
        }
        register_server(current_server)