import bisect
import hashlib

class Node(object):
    def __init__(self, node, name):
        self.node = node
        self.name = name

class ConsistentHash(object):

    def __init__(self, nodes):
        self.arr = []
        self.nodes = nodes

    def hash(self, key):
        key = str(key)
        hsh = hashlib.sha256()
        hsh.update(bytes(key.encode('utf-8')))
        return int(hsh.hexdigest(), 16) % 2**256

    def get_node(self, key):
        hash_key = self.hash(key)
        index = bisect.bisect_left(self.arr, hash_key) % len(self.nodes)
        if index > 0 and self.arr[index - 1] == key:
            raise Exception("collision occurred")
        return self.nodes[index]


    def setKeys(self):
        for node in self.nodes:
            key = self.hash(node.name)
            bisect.insort_left(self.arr, key)