from flask import Flask, request
import consul
import zmq
import time
import sys
from itertools import cycle

import hrw_hashing
import consistent_hashing

app = Flask(__name__)
c = consul.Consul(host="127.0.0.1", port="8500")
servers = []

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


@app.route('/put', methods=['PUT'])
def put():
    key = request.args.get("key")
    value = request.args.get("value")

    if c.kv.put(key, value):
      return 'Input Successful'
    else:
      return 'Input Failure'

@app.route('/get', methods=['GET'])
def get_one():
    val = c.kv.get(request.args.get("key"))[1]
    return {"key": val['Key'], "value":  str(val["Value"], 'utf-8')}


@app.route('/get_all', methods=['GET'])
def get_all():
    allKeys = c.kv.get(key='', keys=True)
    res = []
    for k in allKeys[1]:
        val = c.kv.get(k)
        res.append({"key": val[1]['Key'], "value": str(val[1]['Value'], 'utf-8')})
    return {"Collection": res}

if __name__ == '__main__':
    num = 1
    if len(sys.argv) > 1:
        num = int(sys.argv[1])
        print(f"num_server={num}")

    for server in range(num):
        server_port = "200{}".format(server)
        servers.append(f'tcp://127.0.0.1:{server_port}')
    app.run(host='127.0.0.1', port=5000)


