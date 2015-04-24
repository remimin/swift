# Copyright (c) 2015 OpenStack Foundation
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

import errno
import os
import time
from swift import gettext_ as _
from random import random

from eventlet import Timeout, GreenPool

import swift.common.db
from swift.container.replicator import ContainerReplicator
from swift.container.backend import ContainerBroker, DATADIR, \
    RECORD_TYPE_TRIE_NODE, RECORD_TYPE_OBJECT
from swift.common import ring, internal_client
from swift.common.db import DatabaseAlreadyExists
from swift.common.exceptions import DeviceUnavailable
from swift.common.request_helpers import get_container_shard_path
from swift.common.shardtrie import ShardTrieDistributedBranchException, \
    ShardTrie, DISTRIBUTED_BRANCH
from swift.common.constraints import SHARD_GROUP_COUNT
from swift.common.ring.utils import is_local_device
from swift.common.utils import get_logger, audit_location_generator, \
    config_true_value, dump_recon_cache, ratelimit_sleep, \
    is_container_sharded, to_shard_trie, whataremyips, ismount, hash_path, \
    storage_directory
from swift.common.daemon import Daemon
from swift.common.wsgi import ConfigString
# TODO clean this up (ie move the default conf out of sync)
from swift.container.sync import ic_conf_body


class ContainerSharder(ContainerReplicator):
    """Shards containers."""

    def __init__(self, conf, logger=None):
        self.conf = conf
        self.logger = logger or get_logger(conf, log_route='container-sharder')
        self.devices = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.interval = int(conf.get('interval', 1800))
        self.container_passes = 0
        self.container_failures = 0
        self.containers_running_time = 0
        self.recon_cache_path = conf.get('recon_cache_path',
                                         '/var/cache/swift')
        self.rcache = os.path.join(self.recon_cache_path, "container.recon")
        swift_dir = conf.get('swift_dir', '/etc/swift')
        self.ring = ring.Ring(swift_dir, ring_name='container')
        self.default_port = 6001
        self.port = int(conf.get('bind_port', self.default_port))
        self.root = conf.get('devices', '/srv/node')
        concurrency = int(conf.get('concurrency', 8))
        self.cpool = GreenPool(size=concurrency)

        # internal client
        self.conn_timeout = float(conf.get('conn_timeout', 5))
        request_tries = int(conf.get('request_tries') or 3)
        internal_client_conf_path = conf.get('internal_client_conf_path')
        if not internal_client_conf_path:
            self.logger.warning(
                _('Configuration option internal_client_conf_path not '
                  'defined. Using default configuration, See '
                  'internal-client.conf-sample for options'))
            internal_client_conf = ConfigString(ic_conf_body)
        else:
            internal_client_conf = internal_client_conf_path
        try:
            self.swift = internal_client.InternalClient(
                internal_client_conf, 'Swift Container Sharder', request_tries)
        except IOError as err:
            if err.errno != errno.ENOENT:
                raise
            raise SystemExit(
                _('Unable to load internal client from config: %r (%s)') %
                (internal_client_conf_path, err))

    def _get_local_devices(self):
        self._local_device_ids = set()
        results = set()
        ips = whataremyips()
        if not ips:
            self.logger.error(_('ERROR Failed to get my own IPs?'))
            return
        for node in self.ring.devs:
            if node and is_local_device(ips, self.port,
                                        node['replication_ip'],
                                        node['replication_port']):
                results.add(node['device'])
                self._local_device_ids.add(node['id'])
        return results

    def _find_shard_container_prefix(self, trie, key, account, container,
                                     trie_cache):
        try:
            node = trie[key]
            return trie.root_key
        except ShardTrieDistributedBranchException as ex:
            dist_key = ex.node.root_key
            if dist_key in trie_cache:
                new_trie = trie_cache[dist_key]
            else:
                args = get_container_shard_path(account, container, dist_key)
                info = self.swift.get_container_metadata(*args)
                new_trie = to_shard_trie(info.get('shardtrie'))
                trie_cache[dist_key] = new_trie
            return self._find_shard_container_prefix(new_trie, key, account,
                                                     container, trie_cache)

    def _get_shard_broker(self, prefix, account, container, policy_index):
        """
        Get a local instance of the shard container broker that will be
        pushed out.

        :param prefix: the shard prefix to use
        :param account: the account
        :param container: the container

        :returns: a local shard container broker
        """
        if self.shard_brokers and \
                container in self.shard_brokers:
            return self.shard_brokers[container][1]
        part = self.ring.get_part(account, container)
        node = self.find_local_handoff_for_part(part)
        if not node:
            raise DeviceUnavailable(
                'No mounted devices found suitable to Handoff sharded '
                'container %s in partition %s' % (container, part))
        hsh = hash_path(account, container)
        db_dir = storage_directory(DATADIR, part, hsh)
        db_path = os.path.join(self.root, node['device'], db_dir, hsh + '.db')
        broker = ContainerBroker(db_path, account=account, container=container)
        if not os.path.exists(broker.db_file):
            try:
                broker.initialize(storage_policy_index=policy_index)
            except DatabaseAlreadyExists:
                pass
        if self.shard_brokers is not None:
            self.shard_brokers[container] = part, broker, node['id']
        return broker

    def _generate_object_list(self, objs_or_trie, policy_index, delete=False):
        """
        Create a list of dictionary items ready to be consumed by
        Broker.merge_items()

        :param objs_or_trie: list of objects or ShardTrie object
        :param policy_index: the Policy index of the container
        :param delete: mark the objects as deleted; default False

        :return: A list of item dictionaries ready to be consumed by
                 merge_items.
        """
        objs = list()
        if isinstance(objs_or_trie, ShardTrie):
            for node in objs_or_trie.get_important_nodes():
                if node.flag == DISTRIBUTED_BRANCH:
                    obj = {'name': node.key,
                           'created_at': node.data['timestamp'],
                           'size': 0,
                           'content_type': '',
                           'etag': '',
                           'deleted': 1 if delete else 0,
                           'storage_policy_index': policy_index,
                           'record_type': RECORD_TYPE_TRIE_NODE}
                else:
                    obj = {'name': node.key,
                           'created_at': node.data['timestamp'],
                           'size': node.data['size'],
                           'content_type': node.data['content_type'],
                           'etag': node.data['etag'],
                           'deleted': 1 if delete else 0,
                           'storage_policy_index': policy_index,
                           'record_type': RECORD_TYPE_OBJECT}
                objs.append(obj)
        else:
            for item in objs_or_trie:
                try:
                    obj = {'name': item[0],
                           'created_at': item[1],
                           'size': item[2],
                           'content_type': item[3],
                           'etag': item[4],
                           'deleted': 1 if delete else 0,
                           'storage_policy_index': policy_index,
                           'record_type': RECORD_TYPE_OBJECT}
                except:
                    self.logger.warning(_("Failed to add object %s, not in the"
                                          'right format'),
                                        item[0] if item[0] else str(item))
                else:
                    objs.append(obj)
        return objs

    def _get_and_fill_shard_broker(self, prefix, objs_or_trie, account,
                                   container, policy_index, delete=False):
        """
        Go grabs or creates a new container broker in a handoff partition
        to use as the new shard for the container. It then sets the required
        sharding metadata and adds the objects from either a list (as you get
        from the container backend) or a ShardTrie object.

        :param prefix: The prefix of the shard trie.
        :param objs_or_trie: A list of objects or a ShardTrie to grab the
               objects from.
        :param account: The root shard account (the original account).
        :param container: The root shard container (the original container).
        :param policy_index:
        :param delete:
        :return: A database broker or None (if failed to grab one)
        """
        acct, cont = get_container_shard_path(account, container, prefix)
        try:
            broker = self._get_shard_broker(prefix, acct, cont, policy_index)
        except DeviceUnavailable as duex:
            self.logger.warning(_(str(duex)))
            return None

        if not broker.metadata.get('X-Container-Sysmeta-Shard-Account'):
            broker.update_metadata((
                ('X-Container-Sysmeta-Shard-Account', account),
                'X-Container-Sysmeta-Shard-Container', container))

        objects = self._generate_object_list(objs_or_trie, policy_index, delete)
        broker.merge_items(objects)

        return self.shard_brokers[cont]

    def _deal_with_misplaced_objects(self, broker, misplaced, trie, account,
                                     container, policy_index):
        """

        :param broker: The parent broker to update once misplaced objects have
                       been moved.
        :param misplaced: The list of misplaced objects as returned by
                          build_shard_trie
        :param trie: The trie built from the broker
        :param account: The root account
        :param container: The root container
        :param policy_index: The policy index of the container
        """
        trie_cache = {}
        shard_prefix_to_obj = {}
        for obj, node in misplaced:
            prefix = self._find_shard_container_prefix(trie, obj[0], trie_cache,
                                                       account, container)
            if shard_prefix_to_obj.get(prefix):
                shard_prefix_to_obj[prefix].append(obj)
            else:
                shard_prefix_to_obj[prefix] = list().append(obj)

        self.logger.info(_('preparing to move misplaced objects found'
                           'in %s/%s'), account, container)
        for shard_prefix, obj_list in shard_prefix_to_obj.iteritems():
            part, broker, node_id = self._get_and_fill_shard_broker(
                shard_prefix, obj_list, account, container, policy_index)

            self.cpool.spawn_n(
                self._replicate_object, part, broker.db_file, node_id)
        self.cpool.waitall()

        # wipe out the cache do disable bypass in delete_db
        cleanups = self.shard_cleanups
        self.shard_cleanups = self.shard_brokers = None
        self.logger.info('Cleaning up %d replicated shard containers',
                         len(cleanups))
        for container in cleanups.values():
            self.cpool.spawn_n(self.delete_db, container)
        self.cpool.waitall()

        # Remove the now relocated misplaced items.
        objs = [obj for obj, node in misplaced]
        items = self._generate_object_list(objs, policy_index, delete=True)
        broker.merge_items(items)
        self.logger.info('Finished misplaced shard replication')

    @staticmethod
    def get_shard_root_path(broker):
        """
        Attempt to get the root shard container name and account for the
        container represented by this broker.

        A container shard has 'X-Container-Sysmeta-Shard-{Account,Container}
        set, which will container the relevant values for the root shard
        container. If they don't exist, then it returns the account and
        container associated directly with the broker.

        :param broker:
        :return: account, container of the root shard container or the brokers
                 if it can't be determined.
        """
        account = broker.metadata.get('X-Container-Sysmeta-Shard-Account',
                                      broker.account)
        container = broker.metadata.get('X-Container-Sysmeta-Shard-Container',
                                      broker.container)
        return account, container

    def _post_replicate_hook(self, broker, info, responses):
            return

    def delete_db(self, broker):
        """
        Ensure that replicated sharded databases are only cleaned up at the end
        of the replication run.
        """
        if self.shard_cleanups is not None:
            # this container shouldn't be here, make sure it's cleaned up
            self.shard_cleanups[broker.container] = broker
            return
        return super(ContainerReplicator, self).delete_db(broker)

    def _one_shard_pass(self, reported):
        local_devs = self._get_local_devices()

        all_locs = audit_location_generator(self.devices, DATADIR, '.db',
                                            mount_check=self.mount_check,
                                            logger=self.logger)
        self.logger.info(_('Starting container sharding pass'))
        self.shard_brokers = dict()
        self.shard_cleanups = dict()
        for path, device, partition in all_locs:
            # Only shard local containers.
            if device not in local_devs:
                continue
            broker = ContainerBroker(path)
            if broker.metadata.get('X-Container-Sysmeta-Sharding') is None:
                # Not a shard container
                continue
            root_account, root_container = \
                ContainerSharder.get_shard_root_path(broker)
            trie, misplaced = broker.build_shard_trie()
            if misplaced:
                # There are objects that shouldn't be in this trie, that is to
                # say, they live beyond a distributed node, so we need to move
                # them to the correct node.
                self._deal_with_misplaced_objects(broker, misplaced, trie,
                                                  root_account,
                                                  root_container,
                                                  broker.storage_policy_index)
                self.shard_brokers = dict()
                self.shard_cleanups = dict()

            # TODO: In the next version we need to support shrinking
            #       for this to work we need to be able to follow distributed
            #       nodes, and therefore need to use direct or internal swift
            #       client. For now we are just going to grow based of this
            #       part of the shard trie.
            # UPDATE: self.swift is an internal client, and
            #       self._find_shard_container_prefix will follow the
            #       ditributed path
            candidate_subtries = trie.get_large_subtries(SHARD_GROUP_COUNT)
            if candidate_subtries:
                level, size, node = candidate_subtries[0]
                self.logger.info(_('sharding subtree of size %d on at prefix '
                                   '%s on container %s'), size, node.key,
                                 broker.container)
                split_trie = trie.split_trie(node)

                try:
                    part, new_broker, node_id = self._get_and_fill_shard_broker(
                        split_trie.root_key, split_trie, root_account,
                        root_container, broker.storage_policy_index)
                except DeviceUnavailable as duex:
                    self.logger.warning(_(str(duex)))
                    continue

                self.logger.info(_('Replicating new shard container %s/%s'),
                                 new_broker.account, new_broker.container)
                self.cpool.spawn_n(
                    self._replicate_object, part, new_broker.db_file, node_id)
                self.cpool.waitall()

                # wipe out the cache do disable bypass in delete_db
                cleanups = self.shard_cleanups
                self.shard_cleanups = self.shard_brokers = None
                self.logger.info('Cleaning up %d replicated shard containers',
                                 len(cleanups))

                for container in cleanups.values():
                    self.cpool.spawn_n(self.delete_db, container)
                self.cpool.waitall()

                self.logger.info(_('Cleaning up sharded objects of old '
                                   'container %s/%s'), broker.account,
                                 broker.container)
                items = self._generate_object_list(split_trie,
                                                   broker.storage_policy_index,
                                                   delete=True)
                broker.merge_items(items)
                self.logger.info(_('Finished sharding %s/%s, new shard '
                                   'container %s/%s. Sharded at prefix %s.'),
                                 broker.account, broker.container,
                                 new_broker.account, new_broker.container,
                                 split_trie.root_key)

        self.logger.info(_('Finished container sharding pass'))






        #all_locs = audit_location_generator(self.devices, DATADIR, '.db',
        #                                    mount_check=self.mount_check,
        #                                    logger=self.logger)
        #for path, device, partition in all_locs:
        #    self.container_audit(path)
        #    if time.time() - reported >= 3600:  # once an hour
        #        self.logger.info(
        #            _('Since %(time)s: Container audits: %(pass)s passed '
        #              'audit, %(fail)s failed audit'),
        #            {'time': time.ctime(reported),
        #             'pass': self.container_passes,
        #             'fail': self.container_failures})
        #        dump_recon_cache(
        #            {'container_audits_since': reported,
        #             'container_audits_passed': self.container_passes,
        #             'container_audits_failed': self.container_failures},
        #            self.rcache, self.logger)
        #        reported = time.time()
        #        self.container_passes = 0
        #        self.container_failures = 0
        #    self.containers_running_time = ratelimit_sleep(
        #        self.containers_running_time, self.max_containers_per_second)
        #return reported

    def run_forever(self, *args, **kwargs):
        """Run the container sharder until stopped."""
        reported = time.time()
        time.sleep(random() * self.interval)
        while True:
            self.logger.info(_('Begin container sharder pass.'))
            begin = time.time()
            try:
                self._one_shard_pass(reported)
            except (Exception, Timeout):
                self.logger.increment('errors')
                self.logger.exception(_('ERROR auditing'))
            elapsed = time.time() - begin
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)
            self.logger.info(
                _('Container audit pass completed: %.02fs'), elapsed)
            dump_recon_cache({'container_auditor_pass_completed': elapsed},
                             self.rcache, self.logger)

    def run_once(self, *args, **kwargs):
        """Run the container sharder once."""
        self.logger.info(_('Begin container sharder "once" mode'))
        begin = reported = time.time()
        self._one_shard_pass(reported)
        elapsed = time.time() - begin
        self.logger.info(
            _('Container sharder "once" mode completed: %.02fs'), elapsed)
        dump_recon_cache({'container_sharder_pass_completed': elapsed},
                         self.rcache, self.logger)
