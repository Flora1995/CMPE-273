import consul
import sys

c = consul.Consul()

class Server:
    def _init_(self, port):
        self.di = {}
        self.di["collection"] = []
        self.port = port

    def server(self):
        context = zmq.Context()
        consumer = context.socket(zmq.REP)
        consumer.bind(f"tcp://127.0.0.1:{self.port}")
        while True:
            raw = consumer.recv_json()
            op = raw['op']
            if (op == "PUT"):
                key, value = raw['key'], raw['value']
                print(f"Server_port={port}:key={key},value={value}")
                self.put(key, value)
            if (op == "GET_ONE"):
                key = raw['key']
                res = self.get_key(key)
            if (op == "GET_ALL"):
                res = self.get_all()
            if (op == "DE_REG"):
                self.deregister(self.port)
            print(f"Server_port={self.port}:key={key},value={value},op={op}")
            consumer.send_json(res)

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
        #print(f"num_server={num_server}")

    for each_server in range(num_server):
        port = "200{}".format(each_server)
        #print(f"Starting a server at:{port}...")
        current_server = {
            'server': f"tcp://127.0.0.1:{port}",
            'port': port
        }
        register_server(current_server)