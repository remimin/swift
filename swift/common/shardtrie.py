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
import json
import zlib

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
        self._key = key
        self._data = None
        self._timestamp = None
        self._flag = EMPTY
        self._parent = parent
        self._children = dict()
        self._level = level

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self, key):
        self._key = key

    def full_key(self):
        if self.parent:
            return self.parent.full_key() + self._key
        else:
            return self._key

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, data):
        self._data = data

    @property
    def timestamp(self):
        return self._timestamp

    @timestamp.setter
    def timestamp(self, ts):
        self._timestamp = ts

    @property
    def flag(self):
        return self._flag

    @flag.setter
    def flag(self, flag):
        self._flag = flag

    @property
    def parent(self):
        return self._parent

    @parent.setter
    def parent(self, parent):
        self._parent = parent

    @property
    def children(self):
        return self._children

    @children.setter
    def children(self, children):
        self._children = children

    @property
    def level(self):
        return self._level

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
        self._key = self.full_key()
        self._parent = None
        return self

    def __iter__(self):
        yield self
        for child in sorted(self.children):
            for c in self.children[child]:
                yield c

    def popping_iter(self):
        yield self
        if isinstance(self.parent, Node):
            del self.parent.children[self.key]
        for child in sorted(self.children):
            for c in child.popping_iter():
                yield c

    def count_data_nodes(self):
        count = 0
        if self.has_data():
            count += 1
        for child in sorted(self.children):
            count += self.children[child].count_data_nodes()

        return count

    def add(self, key, data=None, timestamp=None, flag=DATA_PRESENT):
        key_len = len(key)
        full_key_len = self.level - 1

        if not timestamp:
            timestamp = Timestamp(time.time()).internal
        if self.flag == DISTRIBUTED_BRANCH:
            fullkey = self.full_key()
            raise ShardTrieDistributedBranchException(
                "Subtree '%s' has been distributed." % fullkey, fullkey,
                self)
        elif full_key_len == key_len and self.key[-1] == key[-1]:
            # lets double check the value in case something bad has happened
            fullkey = self.full_key()
            if key != fullkey:
                raise ShardTrieException("Found key %s, but it doesn't match"
                                         " %s something weird is going on",
                                         key, fullkey)
            self.timestamp = timestamp
            self.data = data
            self.flag = flag
            return True
        elif full_key_len < key_len:
            next_key = key[full_key_len]
            if next_key not in self.children:
                # node doesn't exit, so create it.
                new_node = Node(next_key, parent=self,
                                level=self.level + 1)
                self.children[next_key] = new_node

            return self.children[next_key].add(key, data, timestamp, flag)

    def get_node(self, key):
        key_len = len(key)
        full_key_len = self.level - 1

        if full_key_len == key_len and self.key[-1] == key[-1]:
            fullkey = self.full_key()
            if key != fullkey:
                raise ShardTrieException("Found key %s, but it doesn't match"
                                         " %s something weird is going on",
                                         key, fullkey)
            return self
        elif self.flag == DISTRIBUTED_BRANCH:
            raise ShardTrieDistributedBranchException(
                "Subtree '%s' has been distributed." % (self.key), self.key,
                self)
        elif full_key_len < key_len:
            next_key = key[full_key_len]
            if next_key not in self.children:
                return None

            return self.children[next_key].get_node(key)

    def get_last_node(self):
        if not self._children:
            return self
        else:
            child_keys = sorted(self._children.keys())
            return self._children[child_keys[-1]].get_last_node()

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
                self._parent = None
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
    def __init__(self, root_key='', level=1, root_node=None, metadata=None):
        if root_node:
            self._root = root_node
        else:
            self._root = Node(root_key, level=level)
        self._metadata = metadata if metadata else dict()

    def __getitem__(self, key):
        return self.get_node(key)

    @property
    def root(self):
        return self._root

    @root.setter
    def root(self, root):
        self._root = root

    @property
    def root_key(self):
        return self._root.key

    @root_key.setter
    def root_key(self, root_key):
        self._root_key = root_key

    def add_metadata(self, key, value):
        self._metadata[key] = value

    @property
    def metadata(self):
        return self._metadata

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

    def get_distributed_nodes(self, key=None, limit=None, marker=None, **kargs):
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

    def get_important_nodes(self, key=None, limit=None, marker=None, **kargs):
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

    def get_last_node(self):
        return self._root.get_last_node()

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
                new_key = subtrie.root[-1]
                node.parent.children[new_key] = subtrie.root
                subtrie.root.key = new_key
                subtrie.root.parent = node.parent

    def dump(self):
        data = self._root.dump()
        data['metadata'] = self._metadata
        return data

    def dump_to_json(self):
        return json.dumps(self.dump())

    def dump_to_zlib(self, level=6):
        return zlib.compress(self.dump_to_json(), level)

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
            # We need to make sure the new root has the correct key (more then
            # a single character).
            new_root.key = new_root.full_key()
            self._root = new_root
            self._root.parent = None


    @staticmethod
    def load(node_dict):
        for key in ('parent', 'key', 'data', 'children', 'timestamp', 'flag'):
            if key not in node_dict:
                raise ShardTrieException('Malformed ShardTrie node dictionary')

        metadata = node_dict.get('metadata', {})
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

        return ShardTrie(root_node=node, metadata=metadata)

    @staticmethod
    def load_from_json(json_string):
        return ShardTrie.load(json.loads(json_string))

    @staticmethod
    def load_from_zlib(zlib_string):
        return ShardTrie.load_from_json(zlib.decompress(zlib_string))

    def get_large_subtries(self, count=30):
        results = []
        for node in self:
            data_count = node.count_data_nodes()
            if data_count > count and node.level > self.root.level:
                results.append((node.level, data_count, node))

        if results:
            return sorted(results, reverse=True)
        return results

def to_shard_trie(trie):
    """
    Helper method to turn the data returned from a GET to the container server
    with a format=trie info into a ShardTrie object. This is useful as the data
    passed back at the moment is json, but in future testing we may need to run
    a compression algorithm on the json data. This method allows us to undo what
    was done to reduce the response size.

    :param trie: trie data as returned of the info; that is info['shardtrie']
    :return: a ShardTrie object
    """
    try:
        trie = ShardTrie.load_from_zlib(trie)
        #trie = ShardTrie.load_from_json(trie)
    except Exception:
        trie = ShardTrie()

    return trie


def shard_trie_to_string(trie):
    """
    Helper method to turn the trie into something easier to pass back in info,
    for debugging perposes, it currently calls the ShardTrie.dump_to_json()
    method, as it need so be a string we can turn back into a ShardTrie.

    There is also a dump_to_zlib(level) method which zlib compresses the json
    dump.

    Even though the dump to string/compress already exists, this helper function
    is used in case we need to do more to it, and gives us one place to modify.

    :param trie: the ShardTrie object to convert to a string.
    :return: a json string.
    """
    return trie.dump_to_zlib()
    #return trie.dump_to_json()
