# Copyright (c) 2010-2012 OpenStack Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Distributed prefix tree implementation classes used for container sharding"""

import time
from swift.common.utils import Timestamp

EMPTY = 0
DATA_PRESENT = 1
NODE_DELETED = 2
DISTRIBUTED_BRANCH = 3


class ShardTrieDistributedBranchException(Exception):
    def __init__(self, msg, key, node):
        Exception.__init__(self, msg)
        self.node_key = key
        self.trie_node = node

    @property
    def key(self):
        return self.node_key

    @property
    def node(self):
        return self.trie_node


class ShardTrieException(Exception):
    pass


class Node():
    def __init__(self, key, parent=None, level=1):
        self.key = key
        self.data = None
        self.timestamp = None
        self.flag = EMPTY
        self.parent = parent
        self.children = dict()
        self.level = level

    @property
    def Key(self):
        return self.key

    @property
    def Data(self):
        return self.data

    @property
    def Timestamp(self):
        return self.timestamp

    @property
    def Flag(self):
        return self.flag

    def has_data(self):
        return self.flag == DATA_PRESENT

    def is_distributed(self):
        return self.flag == DISTRIBUTED_BRANCH

    def is_special_node(self):
        return self.has_data() or self.is_distributed()

    def set_distributed(self):
        new_node = Node(self.key, parent=self.parent, level=self.level)
        new_node.timestamp = Timestamp(time.time()).internal
        new_node.flag = DISTRIBUTED_BRANCH
        self.parent.children[self.key] = new_node
        self.parent = None
        return self

    def __iter__(self):
        for child in sorted(self.children):
            for c in self.children[child]:
                yield c
        yield self

    def count_data_nodes(self):
        count = 0
        if self.has_data():
            count += 1
        for child in self.children:
            count += self.children[child].count_data_nodes()

        return count

    def add(self, key, data=None, timestamp=None, flag=DATA_PRESENT):
        key_len = len(self.key)
        if not timestamp:
            timestamp = Timestamp(time.time()).internal
        if self.flag == DISTRIBUTED_BRANCH:
            raise ShardTrieDistributedBranchException(
                "Subtree '%s' has been distributed." % (self.key), self.key,
                self)
        elif self.key == key:
            self.timestamp = timestamp
            self.data = data
            self.flag = flag
            return True
        elif key_len < len(key):
            next_key = key[:key_len + 1]
            if next_key not in self.children:
                # node doesn't exit, so create it.
                new_node = Node(next_key, parent=self,
                                level=self.level + 1)
                self.children[next_key] = new_node

            return self.children[next_key].add(key, data, timestamp, flag)

    def get_node(self, key):
        key_len = len(self.key)

        if self.flag == DISTRIBUTED_BRANCH:
            raise ShardTrieDistributedBranchException(
                "Subtree '%s' has been distributed." % (self.key), self.key,
                self)
        elif self.key == key:
            return self
        elif key_len < len(key):
            next_key = key[:key_len + 1]
            if next_key not in self.children:
                return None

            return self.children[next_key].get_node(key)

    def get(self, key, full=False):
        node = self.get_node(key)
        if node:
            return self.data if full else self.data['data']

    def delete(self, key):
        key_len = len(self.key)
        if self.key == key:
            if self.children:
                self.timestamp = Timestamp(time.time()).internal
                self.data = None
                self.flag = NODE_DELETED
            else:
                # remove the node
                del self.parent.children[self.key]
                self.data = None
                self.flag = EMPTY
                self.timestamp = None
                self.children = None
                self.parent = None
            return True
        elif key_len < len(key):
            next_key = key[:key_len + 1]
            if next_key not in self.children:
                return False

            return self.children[next_key].delete(key)

    def dump(self, dump_level=False):
        node_dict = {
            'parent': self.parent.key if self.parent else 'None',
            'key': self.key,
            'flag': self.flag,
            'timestamp': self.timestamp if self.timestamp else 'None',
            'data': self.data,
            'children': [],
        }
        if dump_level:
            node_dict['level'] = self.level

        for child in sorted(self.children.keys()):
            child_dict = self.children[child].dump()
            node_dict['children'].append(child_dict)

        return node_dict


class ShardTrie():
    """A distributed prefix tree used for managing container shards

    Nodes have a timestamp which is used for merging trees.
    Throws ShardTrieDistributedBranchException, ShardTriedException
    """
    def __init__(self, root_key='', level=1, root_node=None):
        if root_node:
            self._root = root_node
        else:
            self._root = Node(root_key, level=level)

    def __getitem__(self, key):
        return self.get_node(key)

    @property
    def root(self):
        return self._root

    @property
    def root_key(self):
        return self._root.key

    def __iter__(self):
        for node in self._root:
            yield node

    def add(self, key, data=None, timestamp=None, flag=DATA_PRESENT):
        return self._root.add(key, data, timestamp, flag)

    def get(self, key, full=False):
        return self._root.get(key, full)

    def get_node(self, key):
        return self._root.get_node(key)

    def delete(self, key):
        return self._root.delete(key)

    def get_data_nodes(self, key=None):
        """ Generator returning data only nodes.

        :param key: The key pointing to the part of the tree to start the
                    search from, default is the root.
        """
        if not key:
            node = self._root
        else:
            node = self.get_node(key)

        if node:
            for n in node:
                if n.has_data():
                    yield n

    def get_distributed_nodes(self, key=None):
        """Generator returning distributed tree nodes in the tree.

        :param key: The key pointing to the part of the tree to start the
                    search from, default is the root.
        """
        if not key:
            node = self._root
        else:
            node = self.get_node(key)

        if node:
            for n in node:
                if n.is_distributed():
                    yield n

    def get_important_nodes(self, key=None, limit=None, marker=None):
        """Generator returning but the data and distributed nodes in the tree.

        :param key: The key pointing to the part of the tree to start the
                    search from, default is the root.
        """
        start_yielding = True
        yielded_count = 0

        if not key:
            node = self._root
        else:
            node = self.get_node(key)

        if marker:
            start_yielding = False

        if node:
            for n in node:
                if n.is_distributed() or n.has_data():
                    if limit and yielded_count >= limit:
                        raise StopIteration()
                    if marker and n == marker:
                        start_yielding = True
                        continue
                    if start_yielding:
                        yielded_count += 1
                        yield n


    def split_trie(self, key):
        """Splits the tree at key.

        Places a distributed tree flag and returns the subtree.
        """
        node = self.get_node(key)
        if node:
            split_node = node.set_distributed()
            subTree = ShardTrie(root_node=split_node)
            return subTree

        raise ShardTrieException("Key '%s' not found" % (key))

    def join_subtrie(self, subtrie, force=False):
        if not isinstance(subtrie, ShardTrie):
            raise ShardTrieException("You must pass in a ShardTrie instance")

        key = subtrie.root_key
        node = self.get_node(key)
        if node:
            if node.is_distributed() or force:
                node.parent.children[key] = subtrie.root
                subtrie.root.parent = node.parent

    def dump(self):
        return self._root.dump()

    def trim_trunk(self):
        if len(self._root.children) >= 0 and len(self._root.children) != 1:
            return

        new_root = None
        node = self._root
        while True:
            if len(node.children) == 1 and not node.is_special_node():
                node = node.children.values()[0]
                new_root = node
            else:
                break

        if new_root:
            self._root = new_root
            self._root.parent = None


    @staticmethod
    def load(node_dict):
        for key in ('parent', 'key', 'data', 'children', 'timestamp', 'flag'):
            if key not in node_dict:
                raise ShardTrieException('Malformed ShardTrie node dictionary')

        node = Node(node_dict['key'], level=node_dict.get('level', 1))
        node.data = node_dict['data']
        node.timestamp = node_dict.get('timestamp')
        node.flag = node_dict.get('flag', EMPTY)
        for child in node_dict['children']:
            if not child.get('level'):
                child['level'] = node.level + 1

            child_node = ShardTrie.load(child)
            child_node.parent = node
            node.children[child_node._root.key] = child_node._root
            node.children[child_node._root.key].parent = node

        return ShardTrie(root_node=node)

    def get_large_subtries(self, count=30):
        results = []
        for node in self:
            data_count = node.count_data_nodes()
            if data_count > count and node.level > self.root.level:
                results.append((node.level, data_count, node))

        if results:
            return sorted(results, reverse=True)
        return results