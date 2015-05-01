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
import time
import json
from itertools import chain

from swift.common.utils import public, csv_append, Timestamp, \
    is_container_sharded, config_true_value
from swift.common.constraints import check_metadata
from swift.common import constraints, wsgi, shardtrie
from swift.common.http import HTTP_ACCEPTED, is_success
from swift.common.request_helpers import get_listing_content_type, \
    get_container_shard_path, get_sys_meta_prefix
from swift.common.shardtrie import to_shard_trie
from swift.proxy.controllers.base import Controller, delay_denial, \
    cors_validation, clear_info_cache
from swift.common.storage_policy import POLICIES
from swift.common.swob import HTTPBadRequest, HTTPForbidden, \
    HTTPNotFound, HTTPServerError, Response, Request



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
                'x-remove-versions-location',
                'x-remove-%s-sync-key' % st,
                'x-remove-%s-sync-to' % st]

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

    def GETorHEAD_sharded(self, req, container_info):
        # TODO: build a new respose, loop through the tree. But first need utils
        # to generate the account name and container name etc.
        trie = to_shard_trie(container_info.get('shardtrie'))
        listing_content_type = get_listing_content_type(req)

        responses = list()
        iterator_func = 'get_important_nodes'
        if req.method == "HEAD":
            iterator_func = 'get_distributed_nodes'

        def _run_single_request(account_name, container_name):
            part = self.app.container_ring.get_part(
                account_name, container_name)
            node_iter = self.app.iter_nodes(self.app.container_ring, part)
            if not req.environ.get('swift.skip_sharded'):
                req.environ['swift.skip_sharded'] = True

            path = '/%s/%s' % (account_name, container_name)
            tmp_resp = self.GETorHEAD_base(
                req, _('Container'), node_iter, part, path)

            req.environ['swift.skip_sharded'] = False
            return tmp_resp

        def _get_nodes_from_trie(shard_trie):
            resps = list()
            objects = list()
            for node in getattr(shard_trie,  iterator_func)(**req.str_params):
                if node.is_distributed():
                    acct, cont = get_container_shard_path(self.account_name,
                                                          self.container_name,
                                                          node.key)
                    cont_info = self.container_info(acct, cont)
                    new_trie = to_shard_trie(cont_info['shardtrie'])
                    if new_trie.root_key == shard_trie.root_key:
                        raise HTTPServerError('Loop detected in distributed '
                                              'shard tree')
                    resp = _run_single_request(acct, cont)
                    #if not is_success(resp.status_int):
                    #    return HTTPServerError('failed')
                    responses.append(resp)
                    tmp_resps, tmp_obs = _get_nodes_from_trie(new_trie)
                    resps.extend(tmp_resps)
                    objects.extend(tmp_obs)
                else:
                    # is a data node
                    obj = dict(
                        hash=node.data['etag'],
                        bytes=node.data['size'],
                        content_type=node.data['content_type'],
                        last_modified=node.timestamp,
                        name=node.key
                    )
                    if 'json' in listing_content_type:
                        objects.append(obj)
                    elif 'xml' in listing_content_type:
                        xml = "<object><name>%(name)s</name>"
                        xml += "<hash>%(hash)s</hash><bytes>%(bytes)s</bytes>"
                        xml += "<content_type>%(content_type)s</content_type>"
                        xml += "<last_modified>%(last_modified)s"
                        xml += "</last_modified></object>"
                        objects.append(xml % obj)
                    else:
                        objects.append(obj['name'] + '\n')
            return resps, objects

        # First run the command on the current container
        resp = _run_single_request(self.account_name, self.container_name)
        if not is_success(resp.status_int):
            return resp
        responses.append(resp)

        # Now run it on all shards so parse trie
        try:
            new_responses, objects = _get_nodes_from_trie(trie)
        except HTTPServerError as server_error:
            return server_error
        responses.extend(new_responses)

        # now we can build the response
        resp_status = 500
        resp_headers = None
        object_count = 0
        bytes_used = 0
        for resp in responses:
            if resp.is_success:
                if not resp_headers:
                    resp_headers = resp.headers.copy()
                    resp_status = resp.status
                if is_success(resp.status_int) and resp.status_int != 204:
                    resp_status = resp.status

                object_count += \
                    int(resp.headers.get("X-Container-Object-Count", 0))
                bytes_used += \
                    int(resp.headers.get("X-Container-Bytes-Used", 0))
            else:
                raise HTTPServerError("Failed to talk to container shard")

        resp_headers["X-Container-Bytes-Used"] = str(bytes_used)
        resp_headers["X-Container-Object-Count"] = str(object_count)

        if is_container_sharded(container_info):
            resp_headers['X-Container-Sharding'] = 'On'

        # Generate body
        if 'xml' in listing_content_type:
            head = '<?xml version="1.0" encoding="UTF-8"?>\n'
            head += '<container name="%s">' % self.container_name
            tail = '</container>'
            content_length = sum(map(len, objects))
            content_length += len(head) + len(tail)
            resp_headers["content-length"] = str(content_length)
            return Response(app_iter=chain([head], objects, [tail]),
                            status=resp_status, headers=resp_headers)
        elif 'json' in listing_content_type:
            body = json.dumps(objects)
            resp_headers["content-length"] = str(len(body))
            return Response(body=body, status=resp_status, headers=resp_headers)

        content_length = sum(map(len, objects))
        resp_headers["content-length"] = str(content_length)
        return Response(app_iter=objects, status=resp_status,
                        headers=resp_headers)

    def GETorHEAD(self, req):
        """Handler for HTTP GET/HEAD requests."""
        if not self.account_info(self.account_name, req)[1]:
            if 'swift.authorize' in req.environ:
                aresp = req.environ['swift.authorize'](req)
                if aresp:
                    return aresp
            return HTTPNotFound(request=req)

        container_info = None
        if not req.environ.get('swift.req_info'):
            container_info = self.container_info(self.account_name,
                                                 self.container_name, req)
        if container_info and is_container_sharded(container_info) and \
                not req.environ.get('swift.req_info') and \
                not req.environ.get('swift.skip_sharded'):
            # GETorHEAD_sharded still calls GETorHEAD_base, but seeing as
            # it is sharded there will probably be more then 1 container,
            # therefore needs to send more requests and find more then 1
            # partition to pass GETorHEAD_base.
            resp = self.GETorHEAD_sharded(req, container_info)
        else:
            part = self.app.container_ring.get_part(
                self.account_name, self.container_name)
            node_iter = self.app.iter_nodes(self.app.container_ring, part)
            resp = self.GETorHEAD_base(
                req, _('Container'), node_iter, part,
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
        if self.app.max_containers_per_account > 0 and \
                container_count >= self.app.max_containers_per_account and \
                self.account_name not in self.app.max_containers_whitelist:
            container_info = \
                self.container_info(self.account_name, self.container_name,
                                    req)
            if not is_success(container_info.get('status')):
                resp = HTTPForbidden(request=req)
                resp.body = 'Reached container limit of %s' % \
                    self.app.max_containers_per_account
                return resp
        if req.headers.get('X-Container-Sharding'):
            sysmeta_header = get_sys_meta_prefix('container') + 'sharding'
            req.headers[sysmeta_header] = config_true_value(
                req.headers.get('X-Container-Sharding'))
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
        if req.headers.get('X-Container-Sharding'):
            sysmeta_header = get_sys_meta_prefix('container') + 'sharding'
            if config_true_value(req.headers.get('X-Container-Sharding')):
                req.headers[sysmeta_header] = 'On'
            else:
                req.headers[sysmeta_header] = 'Off'
        container_partition, containers = self.app.container_ring.get_nodes(
            self.account_name, self.container_name)
        headers = self.generate_request_headers(req, transfer=True)
        clear_info_cache(self.app, req.environ,
                         self.account_name, self.container_name)
        resp = self.make_requests(
            req, self.app.container_ring, container_partition, 'POST',
            req.swift_entity_path, [headers] * len(containers))
        return resp

    def _delete_shards(self, trie, req, root=False):
        resp = None
        deleted_nodes = list()
        if not root:
            trie.trim_trunk()
        for node in trie.get_distributed_nodes():
            acct, cont = get_container_shard_path(self.account_name,
                                                  self.container_name,
                                                  node.key)
            cont_info = self.container_info(acct, cont)
            next_trie = to_shard_trie(cont_info['shardtrie'])
            resp = self._delete_shards(next_trie, req)

            if resp is not None and not is_success(resp.status_int):
                return resp

            account_partition, accounts, container_count = \
                self.account_info(acct, req)
            container_partition, containers = \
                self.app.container_ring.get_nodes(acct, cont)
            headers = self._backend_requests(req, len(containers),
                                             account_partition, accounts)
            resp = self.make_requests(
                req, self.app.container_ring, container_partition, 'DELETE',
                req.swift_entity_path, headers)
            if is_success(resp.status_int):
                deleted_nodes.append(node.key)

        if deleted_nodes:
            # Send a post to container to remove these nodes from trie.
            # If root, then use self.container_name etc.
            prefix = None if root else trie.key
            acct, cont = get_container_shard_path(self.account_name,
                                                  self.container_name,
                                                  prefix)
            del_keys_header = "X-Backend-Shard-Del-Keys"
            headers = {del_keys_header: ','.join(deleted_nodes)}
            path = '/v1/%s/%s' % (acct, cont)

            post_req = wsgi.make_subrequest(req.environ, 'POST', path,
                                            headers=headers, swift_source='SH')
            post_resp = self.POST(post_req)
            if not is_success(post_resp.status_int):
                resp = post_resp

        return resp

    @public
    @cors_validation
    def DELETE(self, req):
        """HTTP DELETE request handler."""
        account_partition, accounts, container_count = \
            self.account_info(self.account_name, req)
        if not accounts:
            return HTTPNotFound(request=req)
        # If we are dealing with a sharded container, attempt to run
        # the delete on shard containers first.
        container_info = self.container_info(self.account_name,
                                             self.container_name)
        if is_container_sharded(container_info):
            trie = to_shard_trie(container_info.get('shardtrie', ''))
            resp = self._delete_shards(trie, req, root=True)
            if not resp.is_success():
                return resp

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
