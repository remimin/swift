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

from swift import gettext_ as _
from urllib import unquote
import itertools
import json
import math
import time

from swift.common.utils import public, csv_append, Timestamp, Bitmap, \
    generate_shard_path, generate_shard_container_name, is_container_sharded, \
    config_true_value
from swift.common.constraints import check_metadata, MAX_OBJECTS_PER_SHARD
from swift.common import constraints
from swift.common.http import HTTP_ACCEPTED, is_success
from swift.proxy.controllers.base import Controller, delay_denial, \
    cors_validation, clear_info_cache
from swift.common.exceptions import ShardException
from swift.common.storage_policy import POLICIES
from swift.common.swob import HTTPBadRequest, HTTPForbidden, \
    HTTPNotFound, HTTPServerError, Response


class ContainerController(Controller):
    """WSGI controller for container requests"""
    server_type = 'Container'

    # Ensure these are all lowercase
    pass_through_headers = ['x-container-read', 'x-container-write',
                            'x-container-sync-key', 'x-container-sync-to',
                            'x-versions-location']

    def __init__(self, app, account_name, container_name, **kwargs):
        Controller.__init__(self, app)
        self.account_name = unquote(account_name)
        self.container_name = unquote(container_name)

    def _x_remove_headers(self):
        st = self.server_type.lower()
        return ['x-remove-%s-read' % st,
                'x-remove-%s-write' % st,
                'x-remove-versions-location']

    def _convert_policy_to_index(self, req):
        """
        Helper method to convert a policy name (from a request from a client)
        to a policy index (for a request to a backend).

        :param req: incoming request
        """
        policy_name = req.headers.get('X-Storage-Policy')
        if not policy_name:
            return
        policy = POLICIES.get_by_name(policy_name)
        if not policy:
            raise HTTPBadRequest(request=req,
                                 content_type="text/plain",
                                 body=("Invalid %s '%s'"
                                       % ('X-Storage-Policy', policy_name)))
        if policy.is_deprecated:
            body = 'Storage Policy %r is deprecated' % (policy.name)
            raise HTTPBadRequest(request=req, body=body)
        return int(policy)

    def clean_acls(self, req):
        if 'swift.clean_acl' in req.environ:
            for header in ('x-container-read', 'x-container-write'):
                if header in req.headers:
                    try:
                        req.headers[header] = \
                            req.environ['swift.clean_acl'](header,
                                                           req.headers[header])
                    except ValueError as err:
                        return HTTPBadRequest(request=req, body=str(err))
        return None


    def _sharding(self, req, container_info):
        shard_power = [0]
        bitmap = None
        enable = 'x-container-sharding' in req.headers and \
            config_true_value(req.headers['x-container-sharding'])
        disable = 'x-container-sharding' in req.headers and not \
            config_true_value(req.headers['x-container-sharding'])

        if not enable and not disable:
            # short cut no sharding
            return None

        if enable and disable:
            return HTTPBadRequest(request=req,
                                 content_type="text/plain",
                                 body=("Can not enable and disable sharding in"
                                       "one request"))

        if is_container_sharded(container_info):
            # container sharding activated.. or is it? check for SP of 0
            old_shard_power = \
                container_info.get('sysmeta', {}).get('shard_power')
            if old_shard_power:
                old_shard_power = json.loads(old_shard_power)

                if old_shard_power[-1] == 0 and enable:
                    # Shard_power is turned off, but the reconciler hasn't
                    # done its job yet so append a new shard power to turn
                    # it back on.
                    shard_power = old_shard_power
                    bitmap = Bitmap(
                        container_info.get('sysmeta', {}).get('shard_bitmap'))
                elif old_shard_power[-1] > 0 and disable:
                    # Sharding is definitely on, but we want to turn it off.
                    return self._disable_sharding(req, shard_power)
                else:
                    # shading is on, that we want it on.
                    return None

        if enable:
            return self._enable_sharding(req, container_info, shard_power,
                                         bitmap)


    def _disable_sharding(self, req, shard_power):
        shard_power.append(0)
        req.headers['x-container-sysmeta-part-power'] = \
            json.dumps(shard_power)
        return None


    def _enable_sharding(self, req, container_info, shard_power=[0],
                         bitmap=None):
        num_objects = container_info.get('object_count', 0)
        num_objects = int(num_objects) if num_objects else 0
        objs_per_shard = None
        power = 1
        if 'x-container-meta-max-shard-objects' in req.headers:
            objs_per_shard = req.headers('x-container-meta-max-shard-objects')

            try:
                objs_per_shard = int(objs_per_shard)
            except ValueError:
                return HTTPBadRequest(request=req,
                             content_type="text/plain",
                             body=("Invalid %s '%s' must be an integer"
                                   % ('X-Container-Meta-Max-Shard-Objects',
                                   objs_per_shard)))

        if objs_per_shard:
            req.headers['x-container-meta-max-shard-objects'] = \
                objs_per_shard
        else:
            objs_per_shard = MAX_OBJECTS_PER_SHARD

        if num_objects > 0:
            # Calculate part_power
            num_shards = math.ceil(float(num_objects) /
                                   float(objs_per_shard))
            power = math.ceil(math.log(num_shards, 2))

        shard_power.append(power)
        bitmap = Bitmap(power) if not bitmap else bitmap.set_power(power)
        req.headers['x-container-sysmeta-shard-power'] = \
            json.dumps(shard_power)
        req.headers['x-container-sysmeta-shard-bitmap'] = str(bitmap)

        return None

    def _GET_sharded(self, req, container_info):
        bitmap = Bitmap(bitmap=container_info['sysmeta'].get('shard-bitmap',
                                                             ''))
        if not req.environ.get('swift.already_sharded'):
            req.environ['swift.already_sharded'] = True

        responses = list()
        for shard in itertools.chain([None], bitmap):
            # TODO: Need to take markers and limit into account, that is
            #       Search for the object to find out what shard to look for.
            # part power will not be used because we specify the shard number
            shard, path = generate_shard_path(0, self.account_name,
                                               self.container_name,
                                               shard=shard,
                                               version=False)
            part = self.app.container_ring.get_part(
                self.account_name,
                generate_shard_container_name(shard, self.container_name))

            tmp_resp = self.GETorHEAD_base(
                req, _('Container'), self.app.container_ring, part, path)
            if not is_success(tmp_resp.status_int):
                raise ShardException('Failed to talk to a shard container')
            responses.append(tmp_resp)

        resp_status = 500
        resp_headers = None
        resp_bodies = list()
        object_count = container_info['object_count']
        bytes_used = 0
        content_length = 0
        for resp in responses:
            if resp.is_success:
                resp_bodies.append(resp.body)
                if not resp_headers:
                    resp_headers = resp.headers.copy()
                    resp_status = resp.status
                if is_success(resp.status_int) and resp.status_int != 204:
                    resp_status = resp.status

                bytes_used += \
                    int(resp.headers.get("X-Container-Bytes-Used", 0))
                content_length += \
                    int(resp.headers.get("content-length", 0))
            else:
                raise HTTPServerError("Failed to talk to container shard")
            resp_headers["X-Container-Bytes-Used"] = str(bytes_used)
            resp_headers["content-length"] = str(content_length)
        resp_headers["X-Container-Object-Count"] = str(object_count)

        shard_power = json.loads(container_info['sysmeta'].get('shard-power'))
        if shard_power[-1] != 0:
            resp_headers['X-Container-Sharding'] = 'On'

        # clear the container cache as it gets filled with the info from a shard
        clear_info_cache(self.app, req.environ, self.account_name,
                         self.container_name)

        return Response(app_iter=resp_bodies, status=resp_status,
                        headers=resp_headers)

    def GETorHEAD(self, req):
        """Handler for HTTP GET/HEAD requests."""
        if not self.account_info(self.account_name, req)[1]:
            return HTTPNotFound(request=req)
        part = self.app.container_ring.get_part(
            self.account_name, self.container_name)
        # shortcut HEAD no extra container information required.
        if req.method == 'HEAD':
            resp = self.GETorHEAD_base(
                req, _('Container'), self.app.container_ring, part,
                req.swift_entity_path)
        else:
            # We are in GET, so we need to check to see if this container has
            # container sharding activated
            info = self.container_info(self.account_name, self.container_name)
            if is_container_sharded(info) and \
                    not req.environ.get('swift.already_sharded'):
                # container is sharded
                resp = self._GET_sharded(req, info)
            else:
                resp = self.GETorHEAD_base(
                    req, _('Container'), self.app.container_ring, part,
                    req.swift_entity_path)
        if 'swift.authorize' in req.environ:
            req.acl = resp.headers.get('x-container-read')
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                return aresp
        if not req.environ.get('swift_owner', False):
            for key in self.app.swift_owner_headers:
                if key in resp.headers:
                    del resp.headers[key]
        return resp

    @public
    @delay_denial
    @cors_validation
    def GET(self, req):
        """Handler for HTTP GET requests."""
        return self.GETorHEAD(req)

    @public
    @delay_denial
    @cors_validation
    def HEAD(self, req):
        """Handler for HTTP HEAD requests."""
        return self.GETorHEAD(req)

    @public
    @cors_validation
    def PUT(self, req):
        """HTTP PUT request handler."""
        error_response = \
            self.clean_acls(req) or check_metadata(req, 'container')
        if error_response:
            return error_response
        policy_index = self._convert_policy_to_index(req)
        if not req.environ.get('swift_owner'):
            for key in self.app.swift_owner_headers:
                req.headers.pop(key, None)
        if len(self.container_name) > constraints.MAX_CONTAINER_NAME_LENGTH:
            resp = HTTPBadRequest(request=req)
            resp.body = 'Container name length of %d longer than %d' % \
                        (len(self.container_name),
                         constraints.MAX_CONTAINER_NAME_LENGTH)
            return resp
        account_partition, accounts, container_count = \
            self.account_info(self.account_name, req)
        if not accounts and self.app.account_autocreate:
            self.autocreate_account(req, self.account_name)
            account_partition, accounts, container_count = \
                self.account_info(self.account_name, req)
        if not accounts:
            return HTTPNotFound(request=req)
        container_info = \
                self.container_info(self.account_name, self.container_name,
                                    req)
        if self.app.max_containers_per_account > 0 and \
                container_count >= self.app.max_containers_per_account and \
                self.account_name not in self.app.max_containers_whitelist:
            if not is_success(container_info.get('status')):
                resp = HTTPForbidden(request=req)
                resp.body = 'Reached container limit of %s' % \
                    self.app.max_containers_per_account
                return resp
        # check to see if container sharding is being turned on or off, if so
        # deal with it.
        error_response = self._sharding(req, container_info)
        if error_response:
            return error_response
        container_partition, containers = self.app.container_ring.get_nodes(
            self.account_name, self.container_name)
        headers = self._backend_requests(req, len(containers),
                                         account_partition, accounts,
                                         policy_index)
        clear_info_cache(self.app, req.environ,
                         self.account_name, self.container_name)
        resp = self.make_requests(
            req, self.app.container_ring,
            container_partition, 'PUT', req.swift_entity_path, headers)
        return resp

    @public
    @cors_validation
    def POST(self, req):
        """HTTP POST request handler."""
        error_response = \
            self.clean_acls(req) or check_metadata(req, 'container')
        if error_response:
            return error_response
        if not req.environ.get('swift_owner'):
            for key in self.app.swift_owner_headers:
                req.headers.pop(key, None)
        account_partition, accounts, container_count = \
            self.account_info(self.account_name, req)
        if not accounts:
            return HTTPNotFound(request=req)
        container_info = self.container_info(self.account_name,
                                             self.container_name)
        # check to see if container sharding is being turned on or off and if so
        # deal with it.
        error_response = self._sharding(req, container_info)
        if error_response:
            return error_response
        container_partition, containers = self.app.container_ring.get_nodes(
            self.account_name, self.container_name)
        headers = self.generate_request_headers(req, transfer=True)
        clear_info_cache(self.app, req.environ,
                         self.account_name, self.container_name)
        resp = self.make_requests(
            req, self.app.container_ring, container_partition, 'POST',
            req.swift_entity_path, [headers] * len(containers))
        return resp

    def _make_container_shard_requests(self, req, account_partition, accounts,
                                       container_info):
        resp = None
        success = True
        bitmap = Bitmap(container_info.get('sysmeta', {}).get('shard_bitmap'))
        for shard in bitmap:
            _shard, path = generate_shard_path(0, self.account_name,
                                               self.container_name, shard=shard)
            cont_name = generate_shard_container_name(shard,
                                                      self.container_name)
            container_partition, containers = self.app.container_ring.get_nodes(
            self.account_name, cont_name)
            headers = self._backend_requests(req, len(containers),
                                             account_partition, accounts)
            clear_info_cache(self.app, req.environ,
                             self.account_name, self.container_name)
            resp = self.make_requests(
                req, self.app.container_ring, container_partition, 'DELETE',
                path, headers)
            success = resp.is_success()
            if not success:
                break

        if success:
            # Delete root container
            container_partition, containers = self.app.container_ring.get_nodes(
            self.account_name, self.container_name)
            headers = self._backend_requests(req, len(containers),
                                             account_partition, accounts)
            clear_info_cache(self.app, req.environ,
                             self.account_name, self.container_name)
            resp = self.make_requests(
                req, self.app.container_ring, container_partition, 'DELETE',
                req.swift_entity_path, headers)

        return resp

    @public
    @cors_validation
    def DELETE(self, req):
        """HTTP DELETE request handler."""
        account_partition, accounts, container_count = \
            self.account_info(self.account_name, req)
        if not accounts:
            return HTTPNotFound(request=req)
        container_info = self.container_info(self.account_name,
                                             self.container_name)
        if is_container_sharded(container_info):
            return self._make_container_shard_requests(req, account_partition,
                                                       accounts, container_info)
        container_partition, containers = self.app.container_ring.get_nodes(
            self.account_name, self.container_name)
        headers = self._backend_requests(req, len(containers),
                                         account_partition, accounts)
        clear_info_cache(self.app, req.environ,
                         self.account_name, self.container_name)
        resp = self.make_requests(
            req, self.app.container_ring, container_partition, 'DELETE',
            req.swift_entity_path, headers)
        # Indicates no server had the container
        if resp.status_int == HTTP_ACCEPTED:
            return HTTPNotFound(request=req)
        return resp

    def _backend_requests(self, req, n_outgoing, account_partition, accounts,
                          policy_index=None):
        additional = {'X-Timestamp': Timestamp(time.time()).internal}
        if policy_index is None:
            additional['X-Backend-Storage-Policy-Default'] = \
                int(POLICIES.default)
        else:
            additional['X-Backend-Storage-Policy-Index'] = str(policy_index)
        headers = [self.generate_request_headers(req, transfer=True,
                                                 additional=additional)
                   for _junk in range(n_outgoing)]

        for i, account in enumerate(accounts):
            i = i % len(headers)

            headers[i]['X-Account-Partition'] = account_partition
            headers[i]['X-Account-Host'] = csv_append(
                headers[i].get('X-Account-Host'),
                '%(ip)s:%(port)s' % account)
            headers[i]['X-Account-Device'] = csv_append(
                headers[i].get('X-Account-Device'),
                account['device'])

        return headers
