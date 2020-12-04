import mmh3
import math

def int_to_float(value):
    ones = 0xFFFFFFFFFFFFFFFF >> (64 - 53)
    zeros = float(1 << 53)
    return (value & ones) / zeros

class Node(object):
    def __init__(self, node, weight):
      self.node, self.weight = node, weight

    def get_score(self, key):
        hash_1, hash_2 = mmh3.hash64(str(key), 0xFFFFFFFF)
        score = 1.0 / -math.log(int_to_float(hash_2))
        return score * self.weight

def determine_responsible_node(nodes, key):
    max_score, res = -1, None
    for node in nodes:
        score = node.get_score(key)
        if score > max_score:
            res, max_score = node, score
    return res
