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
import json
import os
import time
import re
from swift import gettext_ as _
from random import random

from eventlet import Timeout

from swift.container.replicator import ContainerReplicator
from swift.container.backend import ContainerBroker, DATADIR, \
    RECORD_TYPE_PIVOT_NODE, RECORD_TYPE_OBJECT
from swift.common import ring, internal_client, db_replicator
from swift.common.bufferedhttp import http_connect
from swift.common.db import DatabaseAlreadyExists
from swift.common.exceptions import DeviceUnavailable, ConnectionTimeout
from swift.common.http import is_success
from swift.common.constraints import CONTAINER_LISTING_LIMIT, \
    SHARD_CONTAINER_SIZE
from swift.common.ring.utils import is_local_device
from swift.common.utils import get_logger, audit_location_generator, \
    config_true_value, dump_recon_cache, whataremyips, hash_path, \
    storage_directory, Timestamp, PivotTree, pivot_to_pivot_container, \
    pivot_container_to_pivot, GreenAsyncPile, ismount
from swift.common.wsgi import ConfigString
from swift.common.storage_policy import POLICIES

# The default internal client config body is to support upgrades without
# requiring deployment of the new /etc/swift/internal-client.conf
ic_conf_body = """
[DEFAULT]
# swift_dir = /etc/swift
# user = swift
# You can specify default log routing here if you want:
# log_name = swift
# log_facility = LOG_LOCAL0
# log_level = INFO
# log_address = /dev/log
#
# comma separated list of functions to call to setup custom log handlers.
# functions get passed: conf, name, log_to_console, log_route, fmt, logger,
# adapted_logger
# log_custom_handlers =
#
# If set, log_udp_host will override log_address
# log_udp_host =
# log_udp_port = 514
#
# You can enable StatsD logging here:
# log_statsd_host = localhost
# log_statsd_port = 8125
# log_statsd_default_sample_rate = 1.0
# log_statsd_sample_rate_factor = 1.0
# log_statsd_metric_prefix =

[pipeline:main]
pipeline = catch_errors proxy-logging cache proxy-server

[app:proxy-server]
use = egg:swift#proxy
account_autocreate = true
# See proxy-server.conf-sample for options

[filter:cache]
use = egg:swift#memcache
# See proxy-server.conf-sample for options

[filter:proxy-logging]
use = egg:swift#proxy_logging

[filter:catch_errors]
use = egg:swift#catch_errors
# See proxy-server.conf-sample for options
""".lstrip()


class ContainerSharder(ContainerReplicator):
    """Shards containers."""

    def __init__(self, conf, logger=None):
        self.conf = conf
        self.logger = logger or get_logger(conf, log_route='container-sharder')
        self.devices = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.interval = int(conf.get('interval', 1800))
        self.per_diff = int(conf.get('per_diff', 1000))
        self.max_diffs = int(conf.get('max_diffs') or 100)
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
        self.vm_test_mode = config_true_value(conf.get('vm_test_mode', 'no'))
        concurrency = int(conf.get('concurrency', 8))
        self.cpool = GreenAsyncPile(concurrency)

        self.node_timeout = int(conf.get('node_timeout', 10))
        self.reclaim_age = float(conf.get('reclaim_age', 86400 * 7))
        self.extract_device_re = re.compile('%s%s([^%s]+)' % (
            self.root, os.path.sep, os.path.sep))
        self.shard_container_size = int(conf.get('shard_container_size',
                                                 SHARD_CONTAINER_SIZE))
        self.rsync_compress = config_true_value(
            conf.get('rsync_compress', 'no'))

        # internal client
        self.conn_timeout = float(conf.get('conn_timeout', 5))
        request_tries = int(conf.get('request_tries') or 3)
        internal_client_conf_path = conf.get('internal_client_conf_path')
        if not internal_client_conf_path:
            self.logger.warning(
                _('Configuration option internal_client_conf_path not '
                  'defined. Using default configuration, See '
                  'internal-client.conf-sample for options. NOTE: '
                  '"account_autocreate = true" is required.'))
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

    def _zero_stats(self):
        """Zero out the stats."""
        # TODO add actual sharding stats to track, and zero them out here.
        self.stats = {'attempted': 0, 'success': 0, 'failure': 0, 'ts_repl': 0,
                      'no_change': 0, 'hashmatch': 0, 'rsync': 0, 'diff': 0,
                      'remove': 0, 'empty': 0, 'remote_merge': 0,
                      'start': time.time(), 'diff_capped': 0}

    def _get_local_devices(self):
        self._local_device_ids = set()
        results = set()
        self.ips = whataremyips()
        if not self.ips:
            self.logger.error(_('ERROR Failed to get my own IPs?'))
            return
        for node in self.ring.devs:
            if node and is_local_device(self.ips, self.port,
                                        node['replication_ip'],
                                        node['replication_port']):
                results.add(node['device'])
                self._local_device_ids.add(node['id'])
        return results

    def _get_pivot_tree(self, account, container, newest=False):
        part, nodes = self.ring.get_nodes(account, container)
        node = self.find_local_handoff_for_part(part)
        hsh = hash_path(account, container)
        db_dir = storage_directory(DATADIR, part, hsh)
        db_path = os.path.join(self.root, node['device'], db_dir, hsh + '.db')
        self.logger.info(_("BY RMM db path:%s"), db_path)

        path = self.swift.make_path(account, container) + \
            '?nodes=pivot&format=json'
        headers = dict()
        if newest:
            headers['X-Newest'] = 'true'
        self.logger.info(_('RMM path:%s'), path) 

        pivot_points = None
        if not os.path.exists(db_path):
            _path = "/%s/%s" %(account, container)
            for node in nodes:
                 self.logger.info(_('RMM ip:%s, port:%s, device:%s, part:%s'),
                                  node['ip'],  node['port'], node['device'], part)
                 resp = self._send_request(node['ip'],  node['port'], node['device'],
                                    part, 'GET', _path, headers, "nodes=pivot&format=json")
                 if not is_success(resp.status):
                     self.logger.info(_('RMM resp status %s'), resp.status)
                     continue
                 pivot_points = resp.read()
                 self.logger.info(_('RMM resp %s'),pivot_points) 
                 break
        else:
            try:
                headers['HTTP_USER_AGENT'] = 'container-sharder' 
                resp = self.swift.make_request('GET', path, headers,
                                               acceptable_statuses=(2,))
                pivot_points = resp.body
            except internal_client.UnexpectedResponse as ex:
                self.logger.error(_("Failed to get pivot points from %s/%s"),
                                  account, container)
                return None

        if not pivot_points:
            self.logger.error(_("RMM Failed to get pivot points from %s/%s"),
                                account, container)
            return None
        self.logger.info(_("BY RMM get nodes, account %s , container %s, reps: %s"),
                           account, container, pivot_points)

        tree = PivotTree()
        try:
            for pivot in json.loads(pivot_points):
                tree.add(pivot['name'])
        except ValueError:
            # Failed to decode the json response
            return None
        return tree

    def _get_shard_broker(self, account, container, policy_index):
        """
        Get a local instance of the shard container broker that will be
        pushed out.

        :param account: the account
        :param container: the container
        :returns: a local shard container broker
        """
        if container in self.shard_brokers:
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

        # Get the valid info into the broker.container, etc
        broker.get_info()
        self.shard_brokers[container] = part, broker, node['id']
        return broker

    def _generate_object_list(self, items, policy_index, delete=False):
        """
        Create a list of dictionary items ready to be consumed by
        Broker.merge_items()

        :param items: list of objects
        :param policy_index: the Policy index of the container
        :param delete: mark the objects as deleted; default False

        :return: A list of item dictionaries ready to be consumed by
                 merge_items.
        """
        objs = list()
        timestamp = None
        for item in items:
            try:
                if delete:
                    # Generate a new delete timestamp based off the existing
                    # created_at, this way we don't clobber other objects that
                    # may exist out there. The idea is, newer object out there
                    # will replace the deleted record, which will be picked up
                    # as a misplaced object and then be pushed and compared to
                    # where it needs to be.
                    ts = Timestamp(item[1])
                    ts.offset += 1
                    timestamp = ts.internal
                obj = {
                    'name': item[0],
                    'created_at': timestamp or item[1]}
                if len(item) > 3:
                    # object item
                    obj.update({
                        'size': item[2],
                        'content_type': item[3],
                        'etag': item[4],
                        'deleted': 1 if delete else 0,
                        'storage_policy_index': policy_index,
                        'record_type': RECORD_TYPE_OBJECT})
                else:
                    # pivot node
                    obj.update({
                        'size': item[2],
                        'content_type': '',
                        'etag': '',
                        'deleted': 1 if delete else 0,
                        'storage_policy_index': 0,
                        'record_type': RECORD_TYPE_PIVOT_NODE})
            except Exception:
                self.logger.warning(_("Failed to add object %s, not in the"
                                      'right format'),
                                    item[0] if item[0] else str(item))
            else:
                objs.append(obj)
        return objs

    def _get_and_fill_shard_broker(self, pivot, weight, items, account,
                                   container, policy_index, delete=False):
        """
        Go grabs or creates a new container broker in a handoff partition
        to use as the new shard for the container. It then sets the required
        sharding metadata and adds the objects from either a list (as you get
        from the container backend) or a ShardTrie object.

        :param pivot: The pivot the shard belongs.
        :param items: A list of objects or pivot points
               objects from.
        :param account: The root shard account (the original account).
        :param container: The root shard container (the original container).
        :param policy_index:
        :param delete:
        :return: A database broker or None (if failed to grab one)
        """
        acct, cont = pivot_to_pivot_container(account, container, pivot,
                                              weight)
        self.logger.info(_('BY RMM  pivot_to_pivot_container %s/ %s'), acct, cont)
        try:
            broker = self._get_shard_broker(acct, cont, policy_index)
        except DeviceUnavailable as duex:
            self.logger.warning(_(str(duex)))
            return None

        if not broker.metadata.get('X-Container-Sysmeta-Shard-Account') \
                and pivot:
            timestamp = Timestamp(time.time()).internal
            broker.update_metadata({
                'X-Container-Sysmeta-Shard-Account': (account, timestamp),
                'X-Container-Sysmeta-Shard-Container': (container, timestamp)})

        objects = self._generate_object_list(items, policy_index, delete)
        broker.merge_items(objects)

        return self.shard_brokers[cont]

    def _misplaced_objects(self, broker, root_account, root_container, pivot,
                           weight):
        """

        :param broker: The parent broker to update once misplaced objects have
                       been moved.
        :param root_account: The root account
        :param root_container: The root container
        :param pivot: The pivot point of the container
        :param weight: The weight (side of the pivot) for this container
        """

        self.logger.info(_('Scanning %s/%s for misplaced objects'),
                         broker.account, broker.container)
        queries = []
        policy_index = broker.storage_policy_index
        query = dict(marker='', end_marker='', prefix='', delimiter='',
                     storage_policy_index=policy_index)
        if len(broker.get_pivot_points()) > 0:
            # It's a sharded node, so anything in the object table
            # is misplaced.
            if broker.get_info()['object_count'] > 0:
                queries.append(query.copy())
        elif pivot is None:
            # This is an unsharded root container, so we don't need to
            # query anything.
            return
        else:
            # it hasn't been sharded and isn't the root container, so we need
            # to look for objects that shouldn't be in the object table.
            # First lets get the bounds of the pivot, for this we need
            # the entire pivot tree.
            tree = self._get_pivot_tree(root_account, root_container,
                                        newest=True)
            self.tree_cache[''] = tree
            gt, lt = tree.get_pivot_bounds(pivot)

            # Because we know what side of the pivot this container rests
            # we can reduce the bounds some more.
            if weight <= 0:
                # Left side, so only objects less than (lt) this containers
                # pivot
                lt = pivot
            else:
                # right side, to greater than (gt) pivot
                gt = pivot

            if lt:
                tmp_q = query.copy()
                tmp_q['marker'] = lt
                queries.append(tmp_q)
            if gt:
                tmp_q = query.copy()
                tmp_q['end_marker'] = gt
                queries.append(tmp_q)

        def run_query(qry):
            objs = broker.list_objects_iter(CONTAINER_LISTING_LIMIT, **qry)
            if not objs:
                return

            # We have a list of misplaced objects, so we better find a home
            # for them
            tree = self.tree_cache.get('')
            if not tree:
                tree = self._get_pivot_tree(root_account, root_container,
                                            newest=True)
                self.tree_cache[''] = tree

            pivot_to_obj = {}
            for obj in objs:
                p, w = tree.get(obj[0])

                if (p, w) in pivot_to_obj:
                    pivot_to_obj[(p, w)].append(obj)
                else:
                    pivot_to_obj[(p, w)] = [obj]
                qry['marker'] = obj[0]

            self.logger.info(_('preparing to move misplaced objects found '
                               'in %s/%s'), broker.account, broker.container)
            for p_and_w, obj_list in pivot_to_obj.iteritems():
                part, new_broker, node_id = self._get_and_fill_shard_broker(
                    p_and_w[0].key, p_and_w[1], obj_list, root_account,
                    root_container, policy_index)

                self.cpool.spawn(
                    self._replicate_object, part, new_broker.db_file, node_id)

                # Remove the now relocated misplaced items.
                items = self._generate_object_list(obj_list, policy_index,
                                                   delete=True)
                broker.merge_items(items)
            any(self.cpool)

            if len(objs) == CONTAINER_LISTING_LIMIT:
                # There could be more, so recurse my pretty
                run_query(qry)

        for query in queries:
            run_query(query)

        # wipe out the cache do disable bypass in delete_db
        cleanups = self.shard_cleanups
        self.shard_cleanups = self.shard_brokers = None
        self.logger.info('Cleaning up %d replicated shard containers',
                         len(cleanups))
        for container in cleanups.values():
            self.cpool.spawn(self.delete_db, container)
        any(self.cpool)
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
        broker.get_info()
        account = broker.metadata.get('X-Container-Sysmeta-Shard-Account')
        if account:
            account = account[0]
        else:
            account = broker.account

        container = broker.metadata.get('X-Container-Sysmeta-Shard-Container')
        if container:
            container = container[0]
        else:
            container = broker.container

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

    def _audit_shard_container(self, broker, pivot, root_account=None,
                               root_container=None):

        self.logger.info(_('Auditing %s/%s'), broker.account, broker.container)
        self.logger.info(_('BY RMM Auditing pivot %s'), pivot)
        continue_with_container = True
        if not root_account or not root_container:
            root_account, root_container = \
                ContainerSharder.get_shard_root_path(broker)

        if root_container == broker.container:
            # This is the root container, and therefore the tome of knowledge,
            # So we must assume it's correct (though I may need to this about
            # this some more).
            return continue_with_container

        root_ok = parent_ok = False
        parent = None
        # Get the root view of the world.
        tree = self._get_pivot_tree(root_account, root_container, newest=True)
        if tree is None:
            # failed to get the root tree. Error out for now.. we may need to
            # quarantine the container.
            self.logger.warning(_("Failed to get a pivot tree from root "
                                  "container %s/%s, it may not exist."),
                                root_account, root_container)
            return False
        node, _weight = tree.get(pivot, leaf=False)
        if node:
            root_ok = True
            # the node exists so now we need to find the parent (if there
            # is one)
            if node.parent is None:
                # Parent is root
                parent_ok = True
            else:
                if node.parent.left == node:
                    weight = -1
                else:
                    weight = 1
                self.logger.info(_("BY RMM parent %s,  node.key %s, weight %s"), node.parent.key, node.key, weight)
                parent = (node.parent.key, weight)

        if parent:
            # We need to check the pivot_nodes.
            acct, cont = pivot_to_pivot_container(root_account, root_container,
                                                  *parent)
            self.logger.info(_("BY RMM acct %s, cont %s"), acct, cont)
            self.logger.info(_("BY RMM get parent tree"))
            parent_tree = self._get_pivot_tree(acct, cont, newest=True)
            if parent_tree:
                self.logger.info(_("BY RMM get parent tree ####"))
                node = tree.get(pivot)
                if node:
                    parent_ok = True

        if parent_ok and root_ok:
            # short circuit
            return continue_with_container

        if not parent_ok and not root_ok:
            # We need to quarantine
            # We may actually never get here.. this needs to be re-thought.
            self.logger.warning(_("Shard container '%s/%s' is being "
                                  "quarantined, neither the root container "
                                  "'%s/%s' or it's parent knows of it's "
                                  "existance"), broker.account,
                                broker.container, root_account, root_container)
            # TODO quarantine the container
            continue_with_container = False
            return continue_with_container

        timestamp = Timestamp(time.time()).internal
        # update root container
        tree.add(pivot)
        level = tree.get_level(pivot)
        pivot_point = [(pivot, timestamp, level)]
        if not parent_ok:
            # Update parent
            if parent:
                acct, cont = pivot_to_pivot_container(root_account,
                                                      root_container,
                                                      *parent)
                self.logger.info(_("Shard container '%s/%s' is missing from "
                                   "its parent container '%s/%s', "
                                   "correcting."),
                                 broker.account, broker.container, acct, cont)
                self._push_pivot_point_to_container(
                    parent[0], parent[1], root_account, root_container,
                    pivot_point, broker.storage_policy_index)
        elif not root_ok:
            # update root container
            self.logger.info(_("Shard container '%s/%s' is missing from "
                               "the root container '%s/%s', correcting."),
                             broker.account, broker.container,
                             root_account, root_container)
            self._push_pivot_point_to_container(None, None, root_account,
                                                root_container, pivot_point,
                                                broker.storage_policy_index)
        return continue_with_container

    def _one_shard_pass(self, reported):
        """
        The main function, everything the sharder does forks from this method.

        The sharder loops through each sharded container on server, on each
        container it:
            - audits the container
            - checks and deals with misplaced items
            - 2 phase sharding
                - Phase 1, if there is no pivot defined, find it, then move
                  to next container.
                - Phase 2, if there is a pivot defined, shard it.
        :param reported:
        """
        self._zero_stats()
        all_locs = audit_location_generator(self.devices, DATADIR, '.db',
                                            mount_check=self.mount_check,
                                            logger=self.logger)
        self.logger.info(_('Starting container sharding pass'))
        dirs = []
        self.shard_brokers = dict()
        self.shard_cleanups = dict()
        self._local_device_ids = set()
        self.ips = whataremyips()
        for node in self.ring.devs:
            if node and is_local_device(self.ips, self.port,
                                        node['replication_ip'],
                                        node['replication_port']):
                if self.mount_check and not ismount(
                        os.path.join(self.root, node['device'])):
                    self.logger.warn(
                        _('Skipping %(device)s as it is not mounted') % node)
                    continue
                datadir = os.path.join(self.root, node['device'], self.datadir)
                if os.path.isdir(datadir):
                    self._local_device_ids.add(node['id'])
                    dirs.append((datadir, node['id']))
        for part, path, node_id in db_replicator.roundrobin_datadirs(dirs):
            broker = ContainerBroker(path)
            sharded = broker.metadata.get('X-Container-Sysmeta-Sharding') or \
                broker.metadata.get('X-Container-Sysmeta-Shard-Account')
            if not sharded:
                # Not a shard container
                continue
            if broker.is_deleted():
                # This container is deleted so we can skip it.
                continue
            self.tree_cache = {}
            root_account, root_container = \
                ContainerSharder.get_shard_root_path(broker)
            self.logger.error(_('By RMM -- root_account %s, root_container %s'),
                                root_account, root_container)
            pivot, weight = pivot_container_to_pivot(root_container,
                                                     broker.container)
            self.logger.error(_('By RMM -- pivot %s, weight %s'),
                                pivot, weight)

            # Before we do any heavy lifting, lets do an audit on the shard
            # container. We grab the root's view of the pivot_points and make
            # sure this container exists in it and in what should be it's
            # parent. If its in both great, If it exists in either but not the
            # other, then this needs to be fixed. If, however, it doesn't
            # exist in either then this container may not exist anymore so
            # quarantine it.
            if not self._audit_shard_container(broker, pivot, root_account,
                                               root_container):
                continue

            # now look and deal with misplaced objects.
            self._misplaced_objects(broker, root_account, root_container,
                                    pivot, weight)

            self.shard_brokers = dict()
            self.shard_cleanups = dict()

            # now we need to see if we need to spend any effort trying to shard,
            # if it's already sharded then we want to finish with the audit and
            # check for misplaced objects.
            already_sharded = len(broker.get_pivot_points()) > 0

            if not already_sharded:
                # Sharding is 2 phase
                # If a pivot point is defined, we shard on it.. if it isn't
                # then we see if we need to find a pivot point and set it for
                # the next parse to shard.
                new_pivot = \
                    broker.metadata.get('X-Container-Sysmeta-Shard-Pivot')
                new_pivot = '' if new_pivot is None else new_pivot[0]

                if new_pivot:
                    # We need to shard on the pivot point
                    self._shard_on_pivot(new_pivot, broker, root_account,
                                         root_container, node_id)
                else:
                    # No pivot, so check to see if a pivot needs to be found.
                    obj_count = broker.get_info()['object_count']
                    if obj_count > self.shard_container_size:
                        self._find_pivot_point(broker)

        # wipe out the cache do disable bypass in delete_db
        cleanups = self.shard_cleanups
        self.shard_cleanups = None
        self.logger.info('Cleaning up %d replicated shard containers',
                         len(cleanups))

        for container in cleanups.values():
            self.cpool.spawn(self.delete_db, container)

        # Now we wait for all threads to finish.
        any(self.cpool)

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

    def _send_request(self, ip, port, contdevice, partition, op, path,
                      headers_out={}, query=None):
            if 'user-agent' not in headers_out:
                headers_out['user-agent'] = 'container-sharder %s' % \
                                            os.getpid()
            if 'X-Timestamp' not in headers_out:
                headers_out['X-Timestamp'] = Timestamp(time.time()).normal
            try:
                with ConnectionTimeout(self.conn_timeout):
                        conn = http_connect(ip, port, contdevice, partition,
                                            op, path, headers_out, query)
                with Timeout(self.node_timeout):
                    response = conn.getresponse()
                    return response
            except (Exception, Timeout) as x:
                self.logger.info(str(x))
                # Need to do something here.
                return None

    def _find_pivot_point(self, broker):
        self.logger.info(_('Started searching for best pivot point for %s/%s'),
                         broker.account, broker.container)

        path = "/%s/%s" % (broker.account, broker.container)
        part, nodes = self.ring.get_nodes(broker.account, broker.container)
        nodes = [d for d in nodes
                 if d['ip'] not in self.ips or
                 d['port'] != self.port]
        obj_count = broker.get_info()['object_count']
        found_pivot = broker.get_info()['pivot_point']

        # Send out requests to get suggested pivots and object counts.
        for node in nodes:
            self.cpool.spawn(
                self._send_request, node['ip'], node['port'], node['device'],
                part, 'HEAD', path)

        successes = 1
        for resp in self.cpool:
            if not resp or not is_success(resp.status):
                continue
            else:
                successes += 1
                if resp.getheader('X-Container-Sysmeta-Shard-Pivot'):
                    # The other node already has a shard point defined, but
                    # the local one does't. Stop the search because I haven't
                    # been updated yet (or wait for a replication.
                    self.logger.warning(_("A container replica has a pivot "
                                          "point defined but local container "
                                          "doesn't. This means container "
                                          "hasn't been updated yet aborting "
                                          "search for pivot point"))
                    return
                if int(resp.getheader('X-Container-Object-Count')) > obj_count:
                    obj_count = int(resp.getheader('X-Container-Object-Count'))
                    found_pivot = resp.getheader('X-Backend-Pivot-Point')

        quorum = self.ring.replica_count / 2 + 1
        if successes < quorum:
            self.logger.info(_('Failed to reach quorum on a pivot point for '
                               '%s/%s'), broker.account, broker.container)
            return
        else:
            # Found a pivot point, so lets update all the other containers
            self.logger.info('path: %s', path)
            headers = {'X-Container-Sysmeta-Shard-Pivot': found_pivot}
            for node in nodes:
                self.cpool.spawn(
                    self._send_request, node['ip'], node['port'],
                    node['device'], part, 'POST', path, headers)

            broker.update_metadata({
                'X-Container-Sysmeta-Shard-Pivot':
                    (found_pivot, Timestamp(time.time()).internal)})

            successes = 1
            for resp in self.cpool:
                if is_success(resp.status):
                    successes += 1
            if successes < quorum:
                self.logger.info(_('Failed to set %s as the pivot point for '
                                   '%s/%s on remote servers'),
                                 found_pivot, broker.account, broker.container)
                return

        self.logger.info(_('Best pivot point for %s/%s is %s'),
                         broker.account, broker.container, found_pivot)

    def _shard_on_pivot(self, pivot, broker, root_account, root_container,
                        node_id):
        is_root = root_container == broker.container
        self.logger.info(_('Asking for quorum on a pivot point %s for '
                           '%s/%s'), pivot, broker.account, broker.container)
        # Before we go and split the tree, lets confirm the rest of the
        # containers have a quorum
        quorum = self.ring.replica_count / 2 + 1
        path = "/%s/%s" % (broker.account, broker.container)
        part, nodes = self.ring.get_nodes(broker.account, broker.container)
        nodes = [d for d in nodes
                 if d['ip'] not in self.ips or
                 d['port'] != self.port]

        # Send out requests to get suggested pivots and object counts.
        for node in nodes:
            self.cpool.spawn(
                self._send_request, node['ip'], node['port'], node['device'],
                part, 'HEAD', path)

        successes = 1
        for resp in self.cpool:
            if not is_success(resp.status):
                continue
            if resp.getheader('X-Container-Sysmeta-Shard-Pivot') == pivot:
                successes += 1

        if successes < quorum:
            self.logger.info(_('Failed to reach quorum on a pivot point for '
                               '%s/%s'), broker.account, broker.container)
            return
        else:
            self.logger.info(_('Reached quorum on a pivot point %s for '
                               '%s/%s'), pivot, broker.account,
                             broker.container)

        # Now that we have quorum we can split.
        self.logger.info(_('sharding at from container %s on pivot %s'),
                         broker.container, pivot)

        new_acct, new_left_cont = pivot_to_pivot_container(
            root_account, root_container, pivot, -1)
        new_acct, new_right_cont = pivot_to_pivot_container(
            root_account, root_container, pivot, 1)

        # pivot points to parent and root, Se we need to make sure we can grab
        # the root tree before we move anything.
        tree = self.tree_cache.get('')
        if not tree:
            if is_root:
                tree = broker.build_pivot_tree()
            else:
                tree = self._get_pivot_tree(root_account, root_container, True)
                if tree is None:
                    self.logger.error(
                        _("Since the audit run of this container and "
                          "now we can't access the root container "
                          "%s/%s aborting."),
                        root_account, root_container)
                    return
            self.tree_cache[''] = tree

        # Make sure the account exists by running a container PUT
        try:
            policy = POLICIES.get_by_index(broker.storage_policy_index)
            headers = {'X-Storage-Policy': policy.name}
            self.swift.create_container(new_acct, new_left_cont,
                                        headers=headers)
        except internal_client.UnexpectedResponse as ex:
            self.logger.warning(_('Failed to put container: %s'),
                                str(ex))

        policy_index = broker.storage_policy_index
        query = dict(marker='', end_marker='', prefix='', delimiter='',
                     storage_policy_index=policy_index)

        # there might be more then CONTAINER_LISTING_LIMIT items in the
        # new shard, if so add all the objects to the shard database.
        def add_other_items(marker, broker_to_update, qry):
            while marker:
                qry['marker'] = marker
                new_items = broker.list_objects_iter(
                    CONTAINER_LISTING_LIMIT, **qry)

                # Add new items
                objects = self._generate_object_list(
                    new_items, broker.storage_policy_index)
                broker_to_update.merge_items(objects)

                # Delete existing (while we have the same view of the items)
                delete_objs = self._generate_object_list(
                    new_items, broker.storage_policy_index, delete=True)
                broker.merge_items(delete_objs)

                if len(new_items) == CONTAINER_LISTING_LIMIT:
                    marker = new_items[-1][0]
                else:
                    marker = ''

        for new_cont, weight in ((new_left_cont, -1), (new_right_cont, 1)):
            q = query.copy()
            if weight < 0:
                q.update({'end_marker': pivot,
                          'include_end_marker': True})
            else:
                q.update({'marker': pivot})
            items = broker.list_objects_iter(CONTAINER_LISTING_LIMIT, **q)
            if len(items) == CONTAINER_LISTING_LIMIT:
                marker = items[-1][0]
            else:
                marker = ''

            try:
                npart, new_broker, node_id = \
                    self._get_and_fill_shard_broker(
                        pivot, weight, items, root_account, root_container,
                        policy_index)

                # Delete the same items from current broker (while we have the
                # same state)
                delete_objs = self._generate_object_list(items, policy_index,
                                                         delete=True)
                broker.merge_items(delete_objs)
            except DeviceUnavailable as duex:
                self.logger.warning(_(str(duex)))
                return

            add_other_items(marker, new_broker, q)

            self.logger.info(_('Replicating new shard container %s/%s'),
                             new_broker.account, new_broker.container)
            self.cpool.spawn(
                self._replicate_object, npart, new_broker.db_file, node_id)
            any(self.cpool)

        # Make sure the new distributed node has been added, to do this we need
        # to know what level it is. To get this we add it to the tree
        tree.add(pivot)
        level = tree.get_level(pivot)

        timestamp = Timestamp(time.time()).internal
        pivot_point = [(pivot, timestamp, level)]
        items = self._generate_object_list(pivot_point, 0)
        broker.merge_items(items)

        if not is_root:
            # Push the new pivot point to the root container,
            # we do this so we can short circuit PUTs.
            self._push_pivot_point_to_container(None, None, root_account,
                                                root_container, pivot_point,
                                                broker.storage_policy_index)

        # Now replicate the container we are working on
        self.logger.info(_('Replicating container %s/%s'),
                             broker.account, broker.container)
        self.logger.error(_('BY RMM container %s , part no %s, db_file %s',),
                           broker.container, part, broker.db_file)
        self.cpool.spawn(
            self._replicate_object, part, broker.db_file, node_id)
        any(self.cpool)


        self.logger.info(_('Finished sharding %s/%s, new shard '
                           'containers %s/%s and %s/%s. Sharded at pivot %s.'),
                         broker.account, broker.container,
                         new_acct, new_left_cont, new_acct, new_right_cont,
                         pivot)

    def _push_pivot_point_to_container(self, pivot, weight, root_account,
                                       root_container, pivot_point,
                                       storage_policy_index):
        # Push the new distributed node to the container.
        part, root_broker, node_id = \
            self._get_and_fill_shard_broker(
                pivot, weight, pivot_point, root_account, root_container,
                storage_policy_index)
        self.cpool.spawn(
            self._replicate_object, part, root_broker.db_file, node_id)
        any(self.cpool)

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
                self.logger.exception(_('ERROR sharding'))
            elapsed = time.time() - begin
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)
            self.logger.info(
                _('Container sharder pass completed: %.02fs'), elapsed)
            dump_recon_cache({'container_sharder_pass_completed': elapsed},
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
