import zmq
import time
import sys
from itertools import cycle

import hrw_hashing
import consistent_hashing

def create_clients(servers):
    producers = {}
    context = zmq.Context()
    for server in servers:
        print(f"Creating a server connection to {server}...")
        producer_conn = context.socket(zmq.PUSH)
        producer_conn.bind(server)
        producers[server] = producer_conn
    return producers


def generate_data_round_robin(servers):
    print("Starting...")
    producers = create_clients(servers)
    pool = cycle(producers.values())
    for num in range(10):
        data = { 'key': f'key-{num}', 'value': f'value-{num}' }
        print(f"Sending data:{data}")
        next(pool).send_json(data)
        time.sleep(1)
    print("Done")


def generate_data_consistent_hashing(servers):
    print("Starting...")
    ## TODO
    producers = create_clients(servers)
    nodes = []
    for key in producers.keys():
        nodes.append(consistent_hashing.Node(producers[key], key))
    client_ring = consistent_hashing.ConsistentHash(nodes)
    client_ring.setKeys()

    for num in range(10):
        data = { 'key': f'key-{num}', 'value': f'value-{num}' }
        print(f"Sending data:{data}")
        node = client_ring.get_node(data['key'])
        node.node.send_json(data)
        time.sleep(1)
    print("Done")

def generate_data_hrw_hashing(servers):
    print("Starting...")
    ## TODO
    producers = create_clients(servers)
    nodes = []
    weight = 1
    for key in producers.keys():
        nodes.append(hrw_hashing.Node(producers[key], weight))
        weight = weight + 1
    for num in range(10):
        data = { 'key': f'key-{num}', 'value': f'value-{num}' }
        print(f"Sending data:{data}")
        node = hrw_hashing.determine_responsible_node(nodes, data['key'])
        node.node.send_json(data)
        time.sleep(1)
    print("Done")


if __name__ == "__main__":
    servers = []
    num_server = 1
    if len(sys.argv) > 1:
        num_server = int(sys.argv[1])
        print(f"num_server={num_server}")

    for each_server in range(num_server):
        server_port = "200{}".format(each_server)
        servers.append(f'tcp://127.0.0.1:{server_port}')

    print("Servers:", servers)
    generate_data_round_robin(servers)
    generate_data_consistent_hashing(servers)
    generate_data_hrw_hashing(servers)