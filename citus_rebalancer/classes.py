class Formation:
    def __init__(self, size, delta, nb_nodes):
        self.size = size
        self.ideal_node_size = self.size / nb_nodes
        self.min_node_size = self.ideal_node_size - ((delta/100) * self.size)
        self.max_node_size = self.ideal_node_size + ((delta/100) * self.size)

        self.nodes = None
        self.big_nodes = None
        self.small_nodes = None

    def set_nodes(self, nodes):
        self.nodes = nodes

    def set_big_nodes(self):
        self.big_nodes = []
        for node in self.nodes:
            if not node.is_too_big() or node.nb_resize > 5:
                continue
            self.big_nodes.append(node)

    def set_small_nodes(self):
        self.small_nodes = [node for node in self.nodes if node.is_too_small()]


class Node:
    def __init__(self, name, size, formation):
        self.size = size
        self.name = name
        self.formation = formation
        self.original_size = size
        self.shard_groups = []
        self.nb_resize = 0

    def __repr__(self):
        return 'Node %s: size: %d' % (self.name, self.size)

    def is_too_big(self):
        return self.size > self.formation.max_node_size

    def is_too_small(self):
        return self.size < self.formation.min_node_size

    def set_size(self, value):
        self.size = value

    def set_resize(self, value):
        self.nb_resize = value

    def set_shard_groups(self, groups):
        self.shard_groups = groups

    def add_shard_group(self, group):
        self.shard_groups.append(group)

class ShardGroup:
    def __init__(self, shards, size, node):
        self.shards = shards
        self.size = size
        self.node = node
        self.to_node = None

    def is_moveable_to_node(self, node):
        if (self.to_node
            or (self.node.size - self.size) < self.node.formation.min_node_size
            or (node.size + self.size) > self.node.formation.max_node_size):
            # node already moved
            # or the current node would become too small compared to the formation
            # or the future node would become too big compared to the formation
            return False

        return True

    def set_to_node(self, node):
        self.to_node = node

    def move_to_node(self, node):
        pass

    def __repr__(self):
        if not self.to_node:
            return 'ShardGroup: %d on Node %s' % (self.shards[0], self.node.name)
        return 'ShardGroup: %d on Node %s, move to %s' % (self.shards[0],
                                                          self.node.name,
                                                          self.to_node.name)
