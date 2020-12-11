import zmq
import sys
from multiprocessing import Process
import consul
import json


class Server:
    def __init__(self, port):
        self.dic_storage = []
        self.c = consul.Consul()
        self.port = port

    def server_connect(self):
        context = zmq.Context()
        consumer = context.socket(zmq.REP)
        consumer.bind(f"tcp://127.0.0.1:{self.port}")
        while True:
            raw = consumer.recv_json()
            op = raw['op']
            key = ""
            value = ""
            if (op == 'PUT'):
                key = raw['key']
                value = raw['value']
                self.put(key, value)
                res = 'Done'
            if (op == 'GET_ONE'):
                key = raw['key']
                res = self.get_key(key)    
            if (op == 'GET_ALL'):
                res = self.get_all()
            if (op == 'DE_REG'):
                port = raw['port']
                self.deregister(port)
                res = 'Done'
            if (op == 'REG'):
                port = raw['port']
                self.c.agent.service.register(
                    service_id="serverid-"+str(port), name="server-"+str(port), port=port)
                res = 'Done'
            print(f"Server_port={self.port}:key={key},value={value}, op={op}")
            consumer.send_json(res)
            

    def register(self):
        server_name = "server-" + str(self.port)
        service_id = "serverid-" + str(self.port)
        self.c.agent.service.register(service_id=service_id, name=server_name, port=self.port)

    def deregister(self, port):
        service_id = "serverid-" + str(port)
        self.c.agent.service.deregister(service_id)

    def get_all(self):
        return self.dic_storage

    def get_key(self, key):
        data = self.dic_storage
        value = ""
        for i in range(len(data)):
            if data[i]["key"] == key:
                value = data[i]['value']
        res = {'key': key,'value': value}
        return res

    def put(self, key, value):
        new_data = {'key': key,'value': value}
        res = self.dic_storage.append(new_data)
        return res

if __name__ == "__main__":
    server_port = int(sys.argv[1])
    server = Server(server_port)
    server.register()
    print(f"Starting a server at:{server_port}...")
    Process(target=server.server_connect, args=()).start()
