# Copyright (c) 2010-2015 OpenStack Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for swift.common.shardtrie"""

import unittest

from swift.common import shardtrie
from swift.common.utils import json


class TestShardTrie(unittest.TestCase):
    """Tests for swift.common.ShardTrie"""

    # json trie for use with testing
    # a (data node)
    # |-b
    #   |-c (data node)
    #   |-d (data node)
    #
    _test_shard_trie = json.dumps(
        {
            "parent": "None",
            "timestamp": "0000000001.00000",
            "flag": 1,
            "key": "a",
            "data": {
                "etag": "x",
                "content_type": "text/plain",
                "size": 0
            },
            "children": [
                {
                    "parent": "a",
                    "timestamp": "None",
                    "flag": 0,
                    "key": "b",
                    "data": None,
                    "children": [
                        {
                            "parent": "b",
                            "timestamp": "0000000000.00000",
                            "flag": 1,
                            "key": "c",
                            "data": {
                                "etag": "x",
                                "content_type": "text/plain",
                                "size": 0
                            },
                            "children": []
                        },
                        {
                            "parent": "b",
                            "timestamp": "0000000002.00000",
                            "flag": 1,
                            "key": "d",
                            "data": {
                                "etag": "x",
                                "content_type": "text/plain",
                                "size": 0
                            },
                            "children": []
                        }
                    ]
                }
            ],
            "metadata": {
                "data_node_count": 3
            }
        })

    def setUp(self):
        self.trie = shardtrie.ShardTrie.load_from_json(self._test_shard_trie)

    def tearDown(self):
        pass

    def test_shardtrie_split(self):
        """Test split trie by full key"""
        trie = self.trie

        subtrie = trie.split_trie('ab')
        self.assertTrue(trie.get_node('ab').is_distributed)
        self.assertEqual(subtrie.root.parent, None)
        self.assertEqual(len(list(trie.get_data_nodes())), 1)
        self.assertEqual(len(list(subtrie.get_data_nodes())), 2)

    def test_shardtrie_split_nonexistant_key(self):
        """Test split trie with non-existant key"""
        trie = self.trie

        with self.assertRaises(shardtrie.ShardTrieException):
            subtrie = trie.split_trie('no-such-key')

    def test_shardtrie_get_node(self):
        """Test getting a node"""
        trie = self.trie

        node_abc = trie.get_node('abc')
        self.assertEqual(node_abc.flag, shardtrie.DATA_PRESENT)

    def test_shardtrie_get_distributed_node(self):
        """Test getting a node from a distributed branch"""
        trie = self.trie
        subtrie = trie.split_trie('ab')

        with self.assertRaises(shardtrie.ShardTrieDistributedBranchException):
            trie.get_node('abc')

    def test_shardtrie_join(self):
        """Test joining trie"""
        trie = self.trie

        subtrie = trie.split_trie('ab')
        self.assertEqual(len(list(trie.get_data_nodes())), 1)

        trie.join_subtrie(subtrie)
        self.assertEqual(len(list(trie.get_data_nodes())), 3)

    def test_shardtrie_join_subtrie_not_trie(self):
        """Test joining trie when subtrie is not an instance of ShardTrie"""
        trie = self.trie
        subtrie = trie.split_trie('ab')

        with self.assertRaises(shardtrie.ShardTrieException):
            trie.join_subtrie({'this_is': 'invalid'})

    def test_shardtrie_popping_iterator(self):
        """Test iterating across the trie while removing visited elements"""
        trie = self.trie

        list(trie.root.popping_iter())
        # We cannot use the following line as is_empty checks that the key
        #  is empty string, popping_iterator does not set the key to ''
        # self.assertTrue(trie.is_empty())
        self.assertEqual(trie.root.children, {})

    def test_shardtrie_delete(self):
        """Test deleting a node from shardtrie"""
        trie = self.trie
        number_data_nodes = trie.root.count_data_nodes()

        # Delete a non data node
        self.assertTrue(trie.delete('ab'))
        self.assertEqual(trie.root.count_data_nodes(), number_data_nodes)

        # Delete a data node
        self.assertTrue(trie.delete('abc'))
        self.assertEqual(trie.root.count_data_nodes(), number_data_nodes - 1)

    def test_shardtrie_add(self):
        """Test adding a node to the shardtrie"""
        trie = self.trie
        number_data_nodes = trie.root.count_data_nodes()

        self.assertTrue(trie.add('abce'))
        self.assertEqual(trie.root.count_data_nodes(), number_data_nodes + 1)

    def test_shardtrie_add_distributed(self):
        """Test adding a node to a distributed shardtrie"""
        trie = self.trie

        trie.split_trie('ab')
        with self.assertRaises(shardtrie.ShardTrieDistributedBranchException):
            trie.add('abce')

    def test_shardtrie_get_important_nodes(self):
        """Test iterating across data nodes"""
        trie = self.trie

        # Iterate over all data nodes
        self.assertTrue(
            all([x.data is not None for x in trie.get_important_nodes()]))

        # Iterate over only 2 data nodes
        self.assertEqual(
            len(list(trie.get_important_nodes(limit=2))),
            2)

        # Iterate starting after node abc
        marker = trie.get_node('abc')
        self.assertEqual(
            list(trie.get_important_nodes(marker=marker))[0].full_key(),
            'abd')

        # Iterate starting at 'abc'
        self.assertEqual(
            list(trie.get_important_nodes('abc'))[0].full_key(),
            'abc')

        # Iterate over a distributed and data node
        subtrie = trie.split_trie('ab')
        self.assertEqual(
            len(list(trie.get_important_nodes())),
            2)

    def test_shardtrie_dump(self):
        """Test adding a node to a distributed shardtrie"""
        trie = self.trie
        self.assertEqual(trie.dump(), json.loads(self._test_shard_trie))

    def test_shardtrie_get_last_node(self):
        """Test getting the last node in the shardtrie"""
        trie = self.trie
        last_node = trie.get_last_node()
        self.assertEqual(last_node.key, 'd')

    def test_shardtrie_get_distributed_nodes(self):
        """Test getting distributed nodes after shard"""
        trie = self.trie

        distributed_nodes = trie.get_distributed_nodes()
        self.assertEqual(len(list(distributed_nodes)), 0)

        subtrie = trie.split_trie('ab')

        distributed_nodes = trie.get_distributed_nodes()
        self.assertEqual(len(list(distributed_nodes)), 1)

    def test_shardtrie_get_large_subtries(self):
        """Test getting subtries over certain size"""
        trie = self.trie

        for i in range(20):
            trie.add('abc%d' % i)
            trie.add('abd%d' % i)

        large_tries = [x[2] for x in trie.get_large_subtries(15)]
        self.assertEqual(large_tries, ['abd', 'abc', 'ab'])

    def test_shardtrie_trim_trunk(self):
        """Test trimming the trunk of the shard trie"""
        trie = self.trie

        # Call on an already trimmed trie
        trie.trim_trunk()

        # Call on an trimmed trie
        subtrie = trie.split_trie('ab')
        subtrie.trim_trunk()

        # Call on an untrimmed trie
        subtrie.add('abef', flag=shardtrie.EMPTY)
        subtrie.add('abefg', 'test data')
        subtrie.trim_trunk()


class TestCountingTrie(unittest.TestCase):
    """Tests for swift.common.CountingTrie"""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def _setup_trie(self, keys=['a', 'abc', 'abd'], group_count=500):
        trie = shardtrie.CountingTrie(max_group_size=group_count)

        for node in keys:
            trie.add(node)

        return trie

    def test_countingtrie_add_distributed(self):
        trie = self._setup_trie(group_count=3)

        trie.add('aa', True)
        self.assertTrue(trie._root._children['a']._children['a']._distributed)

    def test_countingtrie_get_best_candidate(self):
        """Test getting the best candidate found by the counting trie"""
        trie = self._setup_trie(group_count=2)

        self.assertEqual(trie.get_best_candidates(), (2, ['ab']))

        # Check the case were the trie finds more than one candidate
        keys = ['a', 'abc', 'abd', 'cba', 'cbb']
        trie = self._setup_trie(keys=keys, group_count=2)
        self.assertEqual(trie.get_best_candidates(), (2, ['ab', 'cb']))

    def test_countingtrie_new_candidate(self):
        """Test manually adding a best candidate"""
        trie = self._setup_trie()

        trie.new_candidate(9, 'abcdefghi')
        self.assertEqual(trie.get_best_candidates(), (9, ['abcdefghi']))

    def test_countingtrie_misplaced_node(self):
        """Test that counting trie correctly notices misplaced nodes"""
        trie = self._setup_trie()

        # Note: a misplaced node is one that is inserted into a trie but
        # must be placed on a distributed node
        trie.add('abcd', True)
        trie.add('abcde')
        self.assertEqual(len(trie.misplaced), 1)

        trie.clear_misplaced()
        self.assertEqual(len(trie.misplaced), 0)

    def test_countingtrie_remove(self):
        """Test that countingrie correctly prunes smaller subtries"""
        trie = self._setup_trie()

        trie.add('accccc')
        trie.add('addddd')

        self.assertEqual(trie._root._children['a']._children.get('b'), None)

if __name__ == '__main__':
    unittest.main()
