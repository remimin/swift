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

from swift.common.middleware import BaseMiddleware, NONE, APP

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


class PipelineRescanException(Exception):
    pass


def load_item(entrypoint):
    # filter_factory = entrypoint.load()
    # wrapped_fn = filter_factory({}) # pass in a conf
    # item_obj = wrapped_fn(None) # pass in an app
    return entrypoint.load()({})(None)


def get_new_indexes(group_map):

    none = []
    index = 0
    new_group_map = {}
    for group in sorted(group_map.keys()):
        if group == NONE:
            none = group_map[group]
            continue
        top_index = index + len(group_map[group])
        zero_items = [item for item in none
                      if top_index > item >= index]
        top_index += len(zero_items)
        new_group_map[group] = [i for i in range(index, top_index)
                                if i not in zero_items]
        index = top_index

    if none:
        new_group_map[NONE] = none
    return new_group_map


def reorder_groups(pipeline, group_map, new_group_map):
    new_pipeline = [None] * len(pipeline)
    for group in sorted(group_map.keys()):
        old_group = group_map[group]
        new_group = new_group_map[group]
        for i, index in enumerate(old_group):
            if index not in new_group:
                sys.stdout.write(
                    'Middleware %s needs to move to position %d\n' %
                    (pipeline[index]['name'], new_group[i]))
            new_pipeline[new_group[i]] = pipeline[index]
    return new_pipeline, new_group_map


def parse_config(config, build=False):
    """
    Takes a serialized scenario and turns it into a data structure suitable
    for feeding to run_scenario().

    :returns: scenario
    :raises: ValueError on invalid scenario
    """
    filter_eps = dict(
        [(entry.name, entry) for entry in
         pkg_resources.iter_entry_points('paste.filter_factory')])
    app_eps = dict([(entry.name, entry) for entry in
                    pkg_resources.iter_entry_points('paste.app_factory')])
    data = dict(name='', group='', before=[], after=[], requires=[], obj=None)
    pipeline = []
    class_map = dict()
    group_map = dict()

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
                msg = 'Last item in pipeline must be an app'
                if build:
                    sys.stdout.write(msg + '\n')
                else:
                    raise PipelineError(msg)
            # load the item
            obj = load_item(filter_eps[ep])
            obj_name = obj.__class__.__name__
            if item not in class_map:
                class_map[obj_name] = dict(obj=obj, indexes=[len(pipeline)])
            else:
                class_map[obj_name]['indexes'].append(len(pipeline))
            item_data = data.copy()
            item_data['name'] = item
            item_data['obj'] = obj
            if isinstance(obj, BaseMiddleware):
                item_data['group'] = obj.group
                item_data['before'] = obj.before
                item_data['after'] = obj.after
                item_data['requires'] = obj.requires
            else:
                item_data['group'] = NONE

            if item_data['group'] == NONE:
                sys.stderr.write("Middleware %s isn't a member of a group, so "
                                 "it will be up to you to make sure it is in "
                                 "the correct position.\n" % item)

            if item_data['group'] in group_map:
                group_map[item_data['group']].append(len(pipeline))
            else:
                group_map[item_data['group']] = [len(pipeline)]
            pipeline.append(item_data)
        elif ep in app_eps:
            if i != len(p_items) - 1:
                msg = 'Middleware %s is an app. It can only ' % item
                msg += 'appear at the end of the pipeline'
                if build:
                    sys.stdout.write(msg + '\n')
                else:
                    raise PipelineError(msg)
            # load the item
            item_data = data.copy()
            item_data.update({'name': item, 'group': APP})
            if APP in group_map:
                group_map[APP].append(len(pipeline))
            else:
                group_map[APP] = [len(pipeline)]
            pipeline.append(item_data)

        else:
            # item doesn't exist
            raise PipelineError(
                'Middleware %s is missing an entry point', item)

        if group_map.get(APP):
            if len(group_map[APP]) > 1:
                l = len(group_map[APP])
                items = [pipeline[i]['name'] for i in group_map[APP]]
                raise PipelineError("You can only define 1 app, you've "
                                    "defined %d: %s", l, ','.join(items))

    fixed_pipeline = None
    new_group_map = get_new_indexes(group_map)
    pipeline, group_map = reorder_groups(pipeline, group_map, new_group_map)
    # Rebuild class_map
    class_map, _junk = rescan_maps(pipeline, skip_group=True)
    checking_pass(pipeline, class_map, group_map, build)
    sys.stdout.write("Current pipeline: %s\n" % pipe)
    if build:
        calc_items = [item['name'] for item in pipeline]
        if calc_items != p_items:
            fixed_pipeline = ' '.join([item['name'] for item in pipeline])
    return fixed_pipeline


def checking_pass(pipeline, class_map, group_map, build=False):
    try:
        for i, item in enumerate(pipeline):
            if item.get('requires'):
                missing = []
                for require in item['requires']:
                    if require not in class_map:
                        missing.append(require)

                if missing:
                    raise PipelineError('Middleware %s requires %s',
                                        item['name'], ', '.join(missing))

            if item.get('before') and item.get('after'):
                error_items = \
                    set(item['before']).intersection(set(item['after']))
                if error_items:
                    # We are in the weird state that this item needs to be
                    raise PipelineError("Middleware %s has the needs to be "
                                        "before and after %s. This is "
                                        "impossible", item,
                                        ','.join(error_items))

            if item.get('before'):
                indexes = []
                for b in item['before']:
                    if b not in class_map:
                        continue
                    # make sure the obj isn't in a smaller group or we'll
                    # get into an infinite loop (possibly)
                    b_indexes = class_map[b]['indexes']
                    if pipeline[b_indexes[0]]['group'] < item['group']:
                        raise PipelineError("It is impossible to place %s "
                                            "before %s as it is apart of a "
                                            "higher group.", item['name'],
                                            pipeline[b_indexes[0]]['name'])
                    elif pipeline[b_indexes[0]]['group'] > item['group']:
                        continue

                    # We are only dealing with the position of items in the
                    # same group.
                    indexes.extend(b_indexes)

                smaller_index = sorted([ind for ind in indexes if ind < i])
                if smaller_index:
                    sys.stdout.write(
                        'Middleware %s should be placed before %s\n'
                        % (item['name'], pipeline[smaller_index[0]]['name']))
                    if build:
                        pipeline.pop(i)
                        pipeline.insert(smaller_index[0], item)
                        class_map, group_map = rescan_maps(pipeline)
                        raise PipelineRescanException()

            if item.get('after'):
                indexes = []
                for b in item['after']:
                    if b not in class_map:
                        continue
                    # make sure the obj isn't in a greater group or we'll
                    # get into an infinite loop (possibly)
                    a_indexes = class_map[b]['indexes']
                    if pipeline[a_indexes[0]]['group'] > item['group']:
                        raise PipelineError("It is impossible to place %s "
                                            "after %s as it is apart of a "
                                            "lower group.", item['name'],
                                            pipeline[a_indexes[0]]['name'])
                    elif pipeline[a_indexes[0]]['group'] < item['group']:
                        continue

                    indexes.extend(a_indexes)

                bigger_index = [ind for ind in indexes if ind > i].sort()
                if bigger_index:
                    sys.stdout.write(
                        'Middleware %s should be placed after %s\n'
                        % (item['name'], pipeline[bigger_index[0]]['name']))
                    if build:
                        pipeline.pop(i)
                        pipeline.insert(bigger_index[-1] + 1, item)
                        class_map, group_map = rescan_maps(pipeline)
                        raise PipelineRescanException()

            # check for app.
            if item['group'] == APP and i != len(pipeline) -1:
                sys.stdout.write("Middleware %s is the app, this needs to be "
                                 "at the end of pipeline\n")
                if build:
                    pipeline.pop(i)
                    pipeline.insert(len(pipeline), item)
                    class_map, group_map = rescan_maps(pipeline)
                    raise PipelineRescanException()

    except PipelineRescanException:
        checking_pass(pipeline, class_map, group_map, build)


def best_index(index, indexes, before=False):
    res = [i for i in indexes if i < index]
    if res:
        if before:
            return res[0]
        return res[-1]
    else:
        return None


def rescan_maps(pipeline, skip_group=False):
    group_map = dict()
    class_map = dict()
    for i, item in enumerate(pipeline):
        group = item['group']
        if not skip_group:
            if group in group_map:
                group_map[group].append(i)
            else:
                group_map[group] = [i]
        if group != APP:
            obj = item['obj']
            obj_name = obj.__class__.__name__
            if obj_name in class_map:
                class_map[obj_name]['indexes'].append(i)
            else:
                class_map[obj_name] = dict(indexes=[i], obj=obj)
    return class_map, group_map


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
        pipeline = parse_config(config, args.correct)
    except PipelineError as err:
        sys.stderr.write("Invalid config %s: %s\n" %
                         (args.conf_path, err))
        return 1

    if args.correct and pipeline:
        sys.stdout.write("Correct pipeline: %s\n" % pipeline)
    return 0
