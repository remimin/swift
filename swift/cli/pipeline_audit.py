#! /usr/bin/env python
# Copyright (c) 2015 Samuel Merritt <sam@swiftstack.com>
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

"""
This is a tool to check to see if the a pipeline has been setup correctly and
can also be used to generate the correct order of a pipeline.
"""

import pkg_resources
import argparse
import sys
try:
    import ConfigParser as cp
except:
    import configparser as cp

from swift.common.middleware import BaseMiddleware, FIRST, SHORT_CIRCUIT, \
    PRE_AUTH, AUTH, POST_AUTH, LAST, APP

ARG_PARSER = argparse.ArgumentParser(
    description='Audit a pipeline')
ARG_PARSER.add_argument(
    '--correct', '-c', action='store_true',
    help="Print out a correctly ordered pipeline if it's incorrect.")
ARG_PARSER.add_argument(
    'conf_path',
    help="Path to the config file containing a swift pipeline")


class PipelineError(Exception):
    pass


def load_item(entrypoint, app=False):
    if app:
        obj = entrypoint.load()({}, '')
        if not hasattr(obj, 'group'):
            obj.group = APP
        return obj
    # filter_factory = entrypoint.load()
    # wrapped_fn = filter_factory({}) # pass in a conf
    # item_obj = wrapped_fn(None) # pass in an app
    return entrypoint.load()({})(None)


def parse_config(config, build=False):
    """
    Takes a serialized scenario and turns it into a data structure suitable
    for feeding to run_scenario().

    :returns: scenario
    :raises: ValueError on invalid scenario
    """
    filter_eps = dict([(entry.name, entry) for entry in
                       pkg_resources.iter_entry_points('paste.filter_factory')])
    app_eps = dict([(entry.name, entry) for entry in
                    pkg_resources.iter_entry_points('paste.app_factory')])
    data = dict(name='', group='', before=[], after=[], requires=[])
    pipeline = []
    class_map = dict()

    # first lets get the pipeline.
    if not config.has_option('pipeline:main', 'pipeline'):
        raise PipelineError('No pipeline defined. section [pipeline:main] and '
                            'option pipeline required')

    pipe = config.get('pipeline:main', 'pipeline')
    if not pipe:
        raise PipelineError("Can't have an empty pipeline.")

    p_items = pipe.split()
    for i, item in enumerate(p_items):
        # First we need the actual entry point name, not the pipeline name
        if config.has_section('filter:%s' % item):
            section = 'filter:%s' % item
        elif config.has_section('app:%s' % item):
            section = 'app:%s' % item
        else:
            raise PipelineError('Missing configuration for pipeline item %s',
                                item)

        # Get the actual entrypoint name
        try:
            entry_path = config.get(section, 'use')
            if '#' in entry_path:
                ep = entry_path.rsplit('#', 1)[-1]
            elif ':' in entry_path:
                ep = entry_path.rsplit(':', 1)[-1]
            else:
                raise PipelineError()
        except:
            raise PipelineError('Failed to find actual entry point for %s',
                                item)

        # first make sure we have an entry point for this item
        if ep in filter_eps:
            if i == len(p_items) - 1:
                # At the last item, it has to be an app
                raise PipelineError('Last item in pipeline must be an app')
            # load the item
            if item not in class_map:
                class_map[item] = dict(obj=load_item(filter_eps[ep]),
                                       indexes=[len(pipeline)])
            else:
                class_map[item]['indexes'].append(len(pipeline))
            item_obj = class_map[item]['obj']
            item_data = data.copy()
            item_data['name'] = item
            if isinstance(item_obj, BaseMiddleware):
                item_data['group'] = item_obj.group
                item_data['before'] = item_obj.before
                item_data['after'] = item_obj.after
                item_data['requires'] = item_obj.requires
            pipeline.append(item_data)
        elif ep in app_eps:
            if i != len(p_items) - 1:
                raise PipelineError('Item %s is an app. It can only appear at '
                                    'the end of the pipeline', item)
            # load the item
            item_data = data.copy()
            item_data.update({'name': item, 'group': APP})
            pipeline.append(item_data)
        else:
            # item doesn't exist
            raise PipelineError('Item %s is missing an entry point', item)

        # Now the checking and reodering part

    return pipeline


def main(argv=None):
    args = ARG_PARSER.parse_args(argv)

    try:
        with open(args.conf_path) as conf:
            config = cp.RawConfigParser()
            config.readfp(conf)
    except OSError as err:
        sys.stderr.write("Error opening config file %s: %s\n" %
                         (args.conf_path, err))
        return 1

    try:
        pipeline = parse_config(config)
    except PipelineError as err:
        sys.stderr.write("Invalid config %s: %s\n" %
                         (args.conf_path, err))
        return 1

    if args.correct and pipeline:
        sys.stdout.write("Correct pipline should be: %s\n" % pipeline)
    return 0
