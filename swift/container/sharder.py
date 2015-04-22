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

from eventlet import Timeout

import swift.common.db
from swift.container.replicator import ContainerReplicator
from swift.container.backend import ContainerBroker, DATADIR
from swift.common import ring, internal_client
from swift.common.request_helpers import get_container_shard_path
from swift.common.shardtrie import ShardTrieDistributedBranchException
from swift.common.constraints import SHARD_GROUP_COUNT
from swift.common.ring.utils import is_local_device
from swift.common.utils import get_logger, audit_location_generator, \
    config_true_value, dump_recon_cache, ratelimit_sleep, \
    is_container_sharded, to_shard_trie, whataremyips, ismount
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
        self.max_containers_per_second = \
            float(conf.get('containers_per_second', 200))
        swift.common.db.DB_PREALLOCATION = \
            config_true_value(conf.get('db_preallocation', 'f'))
        self.recon_cache_path = conf.get('recon_cache_path',
                                         '/var/cache/swift')
        self.rcache = os.path.join(self.recon_cache_path, "container.recon")
        swift_dir = conf.get('swift_dir', '/etc/swift')
        self.ring = ring.Ring(swift_dir, ring_name='container')
        self.default_port = 6001
        self.port = int(conf.get('bind_port', self.default_port))
        self.root = conf.get('devices', '/srv/node')

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
                internal_client_conf, 'Swift Container Sync', request_tries)
        except IOError as err:
            if err.errno != errno.ENOENT:
                raise
            raise SystemExit(
                _('Unable to load internal client from config: %r (%s)') %
                (internal_client_conf_path, err))

    def _get_local_devices(self):
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

    def _deal_with_misplaced_objects(self, misplaced, trie, account, container):
        trie_cache = {}
        shard_container_to_obj = {}
        for obj, node in misplaced:
            prefix = self._find_shard_container_prefix(trie, obj[0], trie_cache,
                                                       account, container)
            acct, cont = get_container_shard_path(account, container, prefix)
            if shard_container_to_obj.get(cont):
                shard_container_to_obj[cont].append(obj)
            else:
                shard_container_to_obj[cont] = list().append(obj)
            # TODO we now have a dict of cont->objs, we now need to
            #      sync them to where they are suppose to be. (using a
            #      threadpool etc.

    def _one_shard_pass(self, reported):
        local_devs = self._get_local_devices()

        all_locs = audit_location_generator(self.devices, DATADIR, '.db',
                                            mount_check=self.mount_check,
                                            logger=self.logger)
        for path, device, partition in all_locs:
            # Only shard local containers.
            if device not in local_devs:
                continue
            broker = ContainerBroker(path)
            if broker.metadata.get('X-Container-Sysmeta-Sharding') is None:
                # Not a shard container
                continue
            trie, misplaced = broker.build_shard_trie()
            if misplaced:
                # There are objects that shouldn't be in this trie, that is to
                # say, they live beyond a distributed node, so we need to move
                # them to the correct node.
                # TODO: deal with objects in the wrong shard.
                self._deal_with_misplaced_objects(misplaced, trie,
                                                  broker.account,
                                                  broker.container)

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





        all_locs = audit_location_generator(self.devices, DATADIR, '.db',
                                            mount_check=self.mount_check,
                                            logger=self.logger)
        for path, device, partition in all_locs:
            self.container_audit(path)
            if time.time() - reported >= 3600:  # once an hour
                self.logger.info(
                    _('Since %(time)s: Container audits: %(pass)s passed '
                      'audit, %(fail)s failed audit'),
                    {'time': time.ctime(reported),
                     'pass': self.container_passes,
                     'fail': self.container_failures})
                dump_recon_cache(
                    {'container_audits_since': reported,
                     'container_audits_passed': self.container_passes,
                     'container_audits_failed': self.container_failures},
                    self.rcache, self.logger)
                reported = time.time()
                self.container_passes = 0
                self.container_failures = 0
            self.containers_running_time = ratelimit_sleep(
                self.containers_running_time, self.max_containers_per_second)
        return reported

    def run_forever(self, *args, **kwargs):
        """Run the container sharder until stopped."""
        reported = time.time()
        time.sleep(random() * self.interval)
        while True:
            self.logger.info(_('Begin container sharder pass.'))
            begin = time.time()
            try:
                reported = self._one_shard_pass(reported)
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

    def container_audit(self, path):
        """
        Audits the given container path

        :param path: the path to a container db
        """
        start_time = time.time()
        try:
            broker = ContainerBroker(path)
            if not broker.is_deleted():
                broker.get_info()
                self.logger.increment('passes')
                self.container_passes += 1
                self.logger.debug('Audit passed for %s', broker)
        except (Exception, Timeout):
            self.logger.increment('failures')
            self.container_failures += 1
            self.logger.exception(_('ERROR Could not get container info %s'),
                                  path)
        self.logger.timing_since('timing', start_time)
