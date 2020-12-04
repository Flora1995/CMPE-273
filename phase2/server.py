import consul
import sys

c = consul.Consul()
storage_dict = {}
node_num = int(sys.argv[1])

def server(port):
    context = zmq.Context()
    consumer = context.socket(zmq.PULL)
    consumer.connect(f"tcp://127.0.0.1:{port}")

    while True:
        raw = consumer.recv_json()
        op = raw['op']
        if (op == "PUT"):
            key, value = raw['key'], raw['value']
            print(f"Server_port={port}:key={key},value={value}")
            storage_dict["key"] = value
        if (op == "GET_ONE"):
            key = raw['key']
            consumer.send_json({
                'key': key,
                'value': storage_dict["key"]
            })
        if (op == "GET_ALL"):
            res = []
            for key,value in storage_dict.items():
                res.append({"key":key, "value":value})
            consumer.send_json({"collections":res})
        if (op == "Add_Node"):
            c.agent.service.register(name='server-'+node_num,service_id=node_num, port="200{}".format(node_num))
            node_num = node_num + 1
        if (op == "Delete_Node"):
            node_num = node_num - 1
            c.agent.service.deregister(name='server-'+node_num,service_id=node_num, port="200{}".format(node_num))    

def register_server(server):
    c.agent.service.register(
        name='server-'+server['port'],
        service_id='server-'+server['port'],
        port=int(server['port'])
    )

if __name__ == "__main__":
    for index in range(int(sys.argv[1]) - 1):
        c.agent.service.register(name='server-'+index,service_id=index, port="200{}".format(index))
