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
    """Tests for swift.common.utils.ShardTrie"""

    _test_shard_trie_json = json.dumps(
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
        pass

    def tearDown(self):
        pass

    def test_shardtrie_load(self):
        trie = shardtrie.ShardTrie.load_from_json(self._test_shard_trie_json)
        node_abc = trie.get_node('abc')
        self.assertEqual(node_abc.flag, shardtrie.DATA_PRESENT)

    def test_shardtrie_split(self):
        trie = shardtrie.ShardTrie.load_from_json(self._test_shard_trie_json)
        subtrie = trie.split_trie('ab')
        self.assertTrue(trie.get_node('ab').is_distributed)
        self.assertEqual(subtrie.root.parent, None)
        self.assertEqual(len([x for x in trie.get_data_nodes()]), 1)
        self.assertEqual(len([x for x in subtrie.get_data_nodes()]), 2)

if __name__ == '__main__':
    unittest.main()
