from flask import Flask, request
import consul
import amq

app = Flask(__name__)
c = consul.Consul(host="127.0.0.1", port="8500")
producers = {}

def create_clients(servers):
    context = zmq.Context()
    for server in servers:
        print(f"Creating a server connection to {server}...")
        producer_conn = context.socket(zmq.PUSH)
        producer_conn.bind(server)
        producers[server] = producer_conn
    return producers


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
    app.run(host='127.0.0.1', port=5000)

