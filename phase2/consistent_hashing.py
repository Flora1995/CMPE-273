import hashlib
import bisect

class ConsistentHashing:
    def __init__(self, nodes=None):
        self.ring = dict()
        self.list = []
        if nodes:
            for node in nodes:
                self.add_node(node)

    def my_hash(self, key):
        m = hashlib.md5(key.encode('utf-8'))
        return int(m.hexdigest(), 16) % (2**32)

    def add_node(self, node):
        key = self.my_hash(node)
        if key in self.list:
            return
        self.ring[key] = node
        self.list.append(key)
        self.list.sort()

    def remove_node(self, node):
        key = self.my_hash(node)
        del self.ring[key]
        self.list.remove(key)

    # Get node and node position
    def get_node(self, str_key):
        h = self.my_hash(str_key)
        if h > self.list[-1]:
            return self.ring[self.list[0]]
        for i in range(len(self.list)):
            if h <= self.list[i]:
                return self.ring[self.list[i]]
        '''key = self.my_hash(str_key)
        nodes = self.list
        for i in range(len(nodes)):
            node = nodes[i]
            if key <= node:
                return self.ring[node], i
        return self.ring[nodes[0]], 0'''
    def get_node_pos(self, str_key):
        h = self.my_hash(str_key)
        if h > self.list[-1]:
            return 0
        for i in range(len(self.list)):
            if h <= self.list[i]:
                return i

    def get_next_node(self, port):
        i = self.get_node_pos(f"tcp://127.0.0.1:{port}") + 1
        if i >= len(self.list):
            i = i % len(self.list)
        return self.ring[self.list[i]]

    def get_pos(self, key):
        hash_key = self.my_hash(key)
        index = bisect.bisect_left(self.list, hash_key) % len(self.list)
        if index > 0 and self.list[index - 1] == key:
            raise Exception("collision occurred")
        return self.list[index]
        
    def setKeys(self):
        for node in self.list:
            key = self.my_hash(node.name)
            bisect.insort_left(self.list, key)
