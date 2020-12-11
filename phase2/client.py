import zmq
import time
import sys
from itertools import cycle
from consistent_hashing import ConsistentHashing
#from hrw import HWR
import consul
import json


class Client:
    def __init__(self):
        self.servers = []
        self.c = consul.Consul()
        self.update_membership()
        self.hashing_ring = ConsistentHashing(self.servers)

    def update_membership(self):
        serversDict = self.c.agent.services()
        self.servers = []
        for i in serversDict:
            server = serversDict[i]
            port = server['Port']
            self.servers.append(f'tcp://127.0.0.1:{port}')
        self.producers = self.create_clients(self.servers)

    def create_clients(self, servers):
        producers = {}  
        context = zmq.Context()
        for server in servers:
            print(f"Creating a server connection to {server}...")
            producer_conn = context.socket(zmq.REQ)
            producer_conn.connect(server)
            producers[server] = producer_conn
        return producers

    def put_data(self, data):
        print(f"Sending data:{data}")
        node = self.hashing_ring.get_node(data['key'])
        producer_conn = self.producers[node]
        producer_conn.send_json(data)
        res = producer_conn.recv()
        return res

    def get_one(self, data):
        node = self.hashing_ring.get_node(data['key'])
        producer_conn = self.producers[node]
        producer_conn.send_json(data)
        res = producer_conn.recv()
        return res.decode('utf8')

    def get_node_data(self, producer):
        producer.send_json({'op': 'GET_ALL'})
        res = producer.recv()
        return res

    def get_all(self):
        arr = []
        for node in self.producers:
            self.producers[node].send_json({'op':'GET_ALL'})
            data = self.producers[node].recv()
            data = data.decode('utf8')
            arr.append(data)
        res = {"collection":arr}
        return res

    def delete_node(self, port):
        producer_conn = self.producers[f"tcp://127.0.0.1:{port}"]
        all_data = json.loads(self.get_node_data(producer_conn))
        self.hashing_ring.remove_node(f"tcp://127.0.0.1:{port}")
        for data in all_data:
            self.put_data({'key': data['key'], 'value': data['value'], 'op': 'PUT'})
        producer_conn.send_json({'op': 'DE_REG','port':port})
        node = f"tcp://127.0.0.1:{port}"
        del self.producers[node]
        self.servers.remove(node)
    
    def add_node(self, port):
        self.hashing_ring.add_node(f"tcp://127.0.0.1:{port}")
        next_node = self.hashing_ring.get_next_node(port)
        next_conn = self.producers[next_node]
        data = json.loads(self.get_node_data(next_conn))
        for o in data:
            self.put_data({'key': o['key'], 'value': o['value'], 'op': 'PUT'})
        return 'Done'


if __name__ == "__main__":
    client = Client()
    #for num in range(30):
    #    data = { 'key': f'key-{num}', 'value': f'value-{num}','op':'PUT'}
    #    client.put_data(data)
    #client.delete_node(8403)
    client.add_node(8404)
    print(client.get_all())
    print(client.get_one({'key':f"key-2",'value':'','op':'GET_ONE'}))