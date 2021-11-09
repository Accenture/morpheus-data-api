#
# Copyright 2018-2021 Accenture Technology
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import argparse
from collections import namedtuple
import csv
import glob
import jmespath
import json
import logging
import os
import re
import requests
import sys
from urllib import parse as urlparse
from urllib3.exceptions import InsecureRequestWarning
import yaml


logger = logging.getLogger(__name__)

__version__ = '0.0.2'
name = "MorpheusDataApi"


COLOR = namedtuple('color', 'red green yellow blue bold endc none')(*[
    lambda s, u=c: '\033[%sm%s\033[0m' % (u, s)
    if (sys.platform != 'win32' and u != '') else s
    for c in '91,92,93,94,1,0,'.split(',')
])
PREFIX_PATHS = {
    'library/': [
        'instance-types',
        'layouts',
        'container-types',
        'container-templates',
        'container-scripts',
        'cluster-layouts',
        'option-types',
        'option-type-lists',
        'spec-templates'
    ]
}
ENTITY_OVERRIDES = {
    'executeSchedules': 'schedules'
}

PATH_PREFIXES = {}
for prefix, paths in PREFIX_PATHS.items():
    for path in paths:
        PATH_PREFIXES[path] = prefix

NON_API_VARIABLES = [
    '$entity',
    '$entityId',
    '$createPath',
    '$updatePath',
    '$deletePath',
    '$validate',
    '$setName'
]

# name or id is already checked for
REQUIRED_ATTRIBUTES = {
    '/api/library/option-types': ['type', 'fieldName', 'fieldLabel'],
    '/api/library/option-list-types': ['type']
}


class MorpheusDataApiException(Exception):
    """Base Exception Class """


class ConstructorException(MorpheusDataApiException):
    """Constructor Exception """
    pass


class ConfigException(MorpheusDataApiException):
    """Constructor Exception """
    pass


class NullTransformException(MorpheusDataApiException):
    """Jmespath Transform Exception """
    pass


class HttpException(MorpheusDataApiException):
    """HTTP Response Exception"""
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return 'HTTP [%s] %s' % (self.code, self.message)


def print_handler(msg):
    print(msg)


def fatal_handler(msg):
    print_handler(COLOR.red('FATAL: %s' % msg))
    sys.exit(1)


def debug_handler(msg, level=''):
    if os.environ.get('MORPHEUS_DEBUG%s' % level) == 'TRUE':
        print_handler('DEBUG: %s' % msg)
    else:
        logger.debug(msg)


def prompt(val):
    """Prompt user based on val

    eg !prompt:enter a 3 digit number!pattern:[0-9]{3}

    :param val: string containing prompt
    :type path: str
    :return: string supplied by user
    :rtype: str
"""
    prompt_txt = val.replace('!prompt:', '')
    pattern = None
    if '!pattern:' in val:
        (prompt_txt, pattern) = prompt_txt.split('!pattern:', 1)
        prompt_val = None
    while True:
        prompt_val = input(COLOR.bold(prompt_txt + ': '))
        if pattern is not None:
            if re.match('^' + pattern + '$', prompt_val):
                # print('match %s %s' % (pattern, prompt_val))
                break
            else:
                print('value must match pattern %s' % pattern)
                continue
        break
    return prompt_val


def get_api_path(path):
    """Get Morpheus API path from string

    eg optionTypes returns /api/library/option-types

    :param path: string containing path or alias
    :type path: str
    :return: path
    :rtype: str
"""
    # prefix /api/ on path if not supplied
    if not path.startswith('/'):
        parts = path.split('/')
        # deal with mixedCase syntax
        if ('-' not in parts[0] and not parts[0].islower()
           and not parts[0].isupper()):
            parts[0] = re.sub(r'(?<!^)(?=[A-Z])', '-', parts[0]).lower()
        # deal with paths that dont have prefixes eg 'library/'
        if parts[0] in PATH_PREFIXES:
            parts[0] = PATH_PREFIXES[parts[0]] + parts[0]
        path = '/api/' + '/'.join(parts)
    return path


def get_entity_from_path(path, entity=None, single=False):
    """Get Morpheus entity from path

    eg /api/library/option-types returns optionTypes

    :param path: string containing path or alias
    :type path: str
    :key entity: entity name if already known
    :key single: remove 's' from end if True
    :return: entity
    :rtype: str
    """
    if entity is None:
        parts = path.split('?')[0].split('/')[-1].split('-')
        entity = parts[0] + ''.join(x.title() for x in parts[1:])
    entity = ENTITY_OVERRIDES.get(entity, entity)
    if single is True and entity.endswith('s'):
        entity = entity[0:-1]
    return entity


class MorpheusDataApi:

    def __init__(self, **kwargs):
        """Morpheus Data API Class

        :key host: Morpheus hostname or MORPHEUS_HOST env var used
        :key token: Morpheus API token or MORPHEUS_TOKEN env var used
        :key print_msg: print messages on upsert/delete, defaults to 'True'
        :key print_handler: function to print messages
        :key fatal_handler: function to call on HTTP Exception if defined
        :key colorize: colorize messages, defaults to 'True'
        :return: __init__ should return None
        :rtype: None
        """
        kwargs = dict(kwargs)
        defaults = {
            'host': os.environ.get('MORPHEUS_HOST'),
            'token': os.environ.get('MORPHEUS_TOKEN'),
            'ssl_verify': True,
            'print_msg': True,
            'print_handler': print_handler,
            'fatal_handler': None,
            'debug_handler': debug_handler,
            'colorize': True
        }
        for k, v in defaults.items():
            kwargs[k] = kwargs.get(k, v)
        missing = [k for k in ['host', 'token'] if kwargs.get(k) is None]
        if len(missing) > 0:
            raise ConstructorException('%s required' % ', '.join(missing))
        self.host = kwargs['host'].replace('https://', '')
        self.token = kwargs['token']
        self._print_msg = kwargs.get('print_msg') is True
        self.print_handler = kwargs['print_handler']
        self.fatal_handler = kwargs.get('fatal_handler')
        self.debug_handler = kwargs.get('debug_handler')
        self.colorize = kwargs['colorize']
        if os.environ.get('MORPHEUS_SSL_VERIFY') == 'FALSE':
            kwargs['ssl_verify'] = False
        self.ssl_verify = kwargs['ssl_verify']
        if self.ssl_verify is False:
            requests.packages.urllib3.disable_warnings(
                category=InsecureRequestWarning
            )

    def print_msg(self, msg):
        """Default print handler used on create/update/delete

        :param msg: message to print
        :type msg: str
        :return: response from registered print handler
        :rtype: None
        """
        if self._print_msg is True:
            return self.print_handler(msg)

    def call(
        self, path, method=None, data=None, params=None,
        return_code=False, transform=None,
        allowed_codes=[200, 201], get_entity=False
    ):
        """Calls Morpheus API

        :param path: morpheus relative path after /api/
        :type path: str
        :key method: HTTP method, defaults to 'get'
        :key data: dict payload for post or put requests
        :key params: dict of query params
        :key return_code: if 'True' returns HTTP response code and dict
        :key transform: jmespath search query to apply to response dict
        :key allowed_codes: list of allowed HTTP codes, defaults to [200, 201]
        :key get_entity: get the entity object from response
        :return: response dict
        :rtype: dict
        """
        (path, _params) = self.get_path_params(path)

        path = get_api_path(path)

        if isinstance(params, dict):
            _params.update(params)
        _url = 'https://%s%s' % (self.host, path)
        headers = {
            'Authorization': 'Bearer %s' % self.token
        }

        s = requests.Session()
        s.verify = self.ssl_verify

        if isinstance(data, dict) or isinstance(data, list):
            headers['Content-Type'] = 'application/json'
            if method is None:
                method = 'post'
            data = json.dumps(data)

        if method is None:
            method = 'get'

        req = requests.Request(
            method.lower(), _url, data=data, headers=headers, params=_params
        )
        prepped = req.prepare()
        resp = s.send(prepped)
        _url_params = urlparse.urlencode(_params)
        if _url_params != '':
            _url_params = '?' + _url_params
        self.debug_handler('HTTP %s %s%s [%s]' % (
            method.upper(), _url, _url_params, resp.status_code
        ))
        content = resp.content
        if data is not None:
            self.debug_handler('HTTP REQUEST %s' % data, 2)
        self.debug_handler('HTTP RESPONSE %s' % content, 2)
        success = None
        try:
            content = json.loads(content)
            if isinstance(content, dict) and 'success' in content:
                success = content['success']
        except Exception:
            content = content
        if resp.status_code not in allowed_codes or success is False:
            if (isinstance(content, dict)
               and ('errors' in content or 'msg' in content)):
                content = content.get('msg', content.get('errors', content))
            if self.fatal_handler is not None:
                return self.fatal_handler('HTTP %s %s [%s]: %s' % (
                    method.upper(), path, resp.status_code, content
                ))
            raise HttpException(resp.status_code, content)
        if return_code is True:
            return (resp.status_code, content)
        if transform is not None:
            content = self.js(transform, content)
            if content is None:
                raise NullTransformException('null response from transform')
        if get_entity is True:
            if isinstance(content, dict):
                entity = [k for k in content.keys() if k != 'meta'][0]
                content = content[entity]
        return content

    def get(self, path, name, exists=True):
        """Gets Morpheus entity/object from name

        :param path: morpheus relative path after /api/
        :type path: str
        :param name: entity/object name
        :type name: str
        :return: Morpheus entity/object
        :rtype: dict
        """
        d = self.call('%s?name=%s' % (path, name), get_entity=True)
        if not isinstance(d, list) or len(d) == 0:
            if exists is False:
                return None
            if self.fatal_handler is not None:
                return self.fatal_handler('HTTP [404] not found')
            raise HttpException(404, 'not found')
        return d[0]

    def js(self, query, data):
        """Perform jmespath query on data

        :param query: jmespath search query
        :type query: str
        :param data: dict/list
        :type name: dict
        :return: results of jmespath search
        :rtype: dict
        """
        return jmespath.search(query, data)

    def get_entity_from_path(self, path, entity=None, single=False):
        """Get Morpheus entity from path

        eg /api/library/option-types returns optionTypes

        :param path: string containing path or alias
        :type path: str
        :key entity: entity name if already known
        :key single: remove 's' from end if True
        :return: entity
        :rtype: str
        """
        return get_entity_from_path(path, entity, single)

    def get_path_params(self, path):
        """Parse path and params from path

        :param path: URL path
        :type path: str
        :return: path and dict of params
        :rtype: tuple
        """
        parsed = urlparse.urlsplit(path)
        return (parsed.path, dict(urlparse.parse_qsl(parsed.query)))

    def get_name_ids(self, path, entity=None, starts_with=None):
        """Returns dict of name, entity/object IDs

        :param path: morpheus relative path after /api/
        :type path: str
        :key entity: entity
        :key start_with: filter names based on prefix
        :return: dict of name and entity/object IDs
        :rtype: dict
        """
        name_ids = {}
        (_path, params) = self.get_path_params(path)
        if isinstance(starts_with, str):
            params['phrase'] = starts_with
        path = _path + '?' + urlparse.urlencode(params)
        for d in self.call(path, get_entity=True):
            id = d.get('id')
            name = d.get('name')
            if (isinstance(starts_with, str)
               and (not isinstance(name, str)
               or not name.startswith(starts_with))):
                continue
            name_ids[name] = id
        return name_ids

    def expand_str(self, d, _cache=None):
        """Expands ${var} variables within nested object

        Eg:
        {
            "fooId": "${id:/api/library/option-types:foo}"
        }

        will value of "fooId" with the ID of option type with name foo

        :param d: object to expand
        :type path: dict, list, str
        :key _cache: internal cache
        :return: expanded object
        :rtype: dict, list, str
        """
        if _cache is None:
            _cache = {}

        if isinstance(d, dict):
            for k, v in d.items():
                d[k] = self.expand_str(v, _cache)

        elif isinstance(d, list):
            for i in range(len(d)):
                d[i] = self.expand_str(d[i], _cache)

        elif isinstance(d, str) and '${' in d:
            orig_d = d
            vars = sorted(list(set(re.findall('[$]{[^}]+}', d))))
            preserve_type = False
            for var in vars:
                # only single value, so preserve type, eg ${id::}
                # returns integer which is needed in APIs
                if var == orig_d:
                    preserve_type = True
                val = None
                if var not in _cache:
                    parts = var[2:-1].split(':')
                    # get entity ID from name: id:path:name
                    if parts[0] == 'id' and len(parts) == 3:
                        val = self.get(parts[1], parts[2]).get('id')
                    _cache[var] = val
                else:
                    val = _cache[var]
                if val is not None:
                    if preserve_type is True:
                        d = val
                        break
                    d = d.replace(var, str(val))
        return d

    def get_entity_id(self, path, name, entity_id=None):
        """Gets Morpheus entity/object ID from name

        :param path: morpheus relative path after /api/
        :type path: str
        :param name: entity/object name
        :type name: str
        :key entity: entity
        :key entity_id: Morpheus entity/object ID
        :return: Morpheus entity/object ID or None if not existing
        :rtype: str
        """
        if entity_id is None:
            entity_d = self.get(path, name, exists=False)
            if isinstance(entity_d, dict) and 'id' in entity_d:
                entity_id = entity_d['id']
        return entity_id

    def upsert(
        self, path, name, data, entity=None, entity_id=None, set_name=True,
        create_path=None, update_path=None,
    ):
        """Creates or Updates data in morpheus keyed on entity name

        :param path: morpheus relative path after /api/
        :type path: str
        :param name: entity/object name
        :type name: str
        :param data: entity data to create/update
        :type data: dict
        :key entity: entity
        :key entity_id: Morpheus entity/object ID
        :key set_name: set name within entity in data
        :return: Morpheus entity/object ID
        :rtype: str
        """
        entity = get_entity_from_path(path, entity)
        entity_id = self.get_entity_id(path, name, entity_id)
        operation = None
        if set_name is True:
            data_entity = entity[0:-1] if entity[-1] == 's' else entity
            if data_entity not in data:
                data = {
                    data_entity: data
                }
            data[data_entity]['name'] = name

        if entity_id is None:
            r = self.call(create_path or path, 'post', data)
            if 'id' in r:
                entity_id = r['id']
            # api post responses are not consistent, so have to search for it
            else:
                entity = [k for k in r.keys() if isinstance(r[k], dict)][0]
                if 'id' not in r[entity]:
                    raise MorpheusDataApiException(
                        'entity id not found in POST response for %s' % (
                           get_api_path(create_path or path)
                        )
                    )
                entity_id = r[entity]['id']
            operation = 'created'
        else:
            self.call('%s/%s' % (update_path or path, entity_id), 'put', data)
            operation = 'updated'
        if name is None:
            name = entity_id
        self.print_msg('%s %s %s [%s]' % (
            operation,
            entity[0:-1] if entity[-1] == 's' else entity,
            COLOR.blue(name) if self.colorize else name,
            entity_id
        ))
        return entity_id

    def delete(self, path, name, entity=None, entity_id=None, force=False):
        """Deletes entity from Morpheus based on name

        :param path: morpheus relative path after /api/
        :type path: str
        :param name: entity/object name
        :type name: str
        :param data: entity data to create/update
        :type data: dict
        :key entity: entity
        :key entity_id: Morpheus entity/object ID
        :key force: force delete from Morpheus, defaults to 'False'
        :return: Morpheus entity/object ID
        :rtype: str
        """
        entity = get_entity_from_path(path, entity)
        entity_id = self.get_entity_id(path, name, entity_id)
        if entity_id is not None:
            _path = path + '/%s' % entity_id
            if force is True:
                _path += '?force=true'
            self.call(_path, 'delete')
            self.print_msg('deleted %s%s [%s]' % (
                entity[0:-1] if entity[-1] == 's' else entity,
                ' ' + (COLOR.blue(name) if self.colorize else name)
                if name is not None else '',
                entity_id
            ))
        return entity_id

    def delete_ids(self, path, ids, force=False):
        """Deletes multiple entity/objects from Morpheus

        :param path: morpheus relative path after /api/
        :type path: str
        :param ids: list of Morpheus entity/object IDs
        :type name: list
        :key force: force delete from Morpheus, defaults to 'False'
        :return: list of IDs deleted
        :rtype: list
        """
        deleted_ids = []
        if isinstance(ids, list):
            for id in ids:
                deleted_ids.append(
                    self.delete(path, None, entity_id=id, force=force)
                )
        return [x for x in deleted_ids if x is not None]

    def get_deploy_ops(
        self, data, pd=None, pk=None,
        config_dir=None, reverse_list=False
    ):
        """Get list of operations to deploy/undeploy

        :param data: nested dict
        :type data: dict
        :key pd: parent dict
        :key pk: parent key
        :key config_dir: str of path to config dir
        :key reverse_list: bool
        :return: list of operations
        :rtype: list
        """

        def _validate_config_var(k, val, _type, keys=None, ext=None):
            msg = '%s.%s' % (pk or 'root', k)
            if _type == 'file':
                msg += ' %s must be %sfile in same dir as yaml config' % (
                    val, ext + ' ' if ext else ''
                )
                if (not isinstance(val, str)
                   or (ext is not None and not val.endswith('.' + ext))):
                    raise ConfigException(msg)
                if not os.path.isfile(val):
                    if config_dir is None:
                        raise ConfigException(msg + ', config_dir required')
                    val = os.path.join(config_dir, val)
                if not os.path.isfile(val):
                    raise ConfigException(msg)
            elif _type in ['$id', '$ids']:
                errmsg = '%s must of type str or list in %s not %s' % (
                    msg, 'format ${id:path:name}', val
                )
                _vals = val if isinstance(val, list) else [val]
                for i in range(len(_vals)):
                    parts = []
                    if isinstance(_vals[i], str):
                        if (_vals[i].startswith('${id:')
                           and _vals[i][-1] == '}'):
                            _vals[i] = _vals[i][5:-1]
                        parts = [
                            x.strip() for x in _vals[i].split(':', 1)
                            if x.strip() != ''
                        ]
                    if len(parts) < 2:
                        raise ConfigException(errmsg)
                    _vals[i] = '${id:%s:%s}' % (get_api_path(
                        parts[0]), parts[1]
                    )
                if len(_vals) == 0:
                    raise ConfigException(errmsg)
                val = _vals[0] if _type == '$id' else _vals
            elif not isinstance(val, _type):
                raise ConfigException('%s must be of type %s not %s' % (
                    msg,  _type.__name__, type(val).__name__
                ))
            if _type == dict:
                if val.pop('$validate', None) is False:
                    return val
                req = {k: [] for k in ['any', 'all']}
                for k in keys:
                    if k[0] == '|':
                        req['any'].append(k[1:])
                    else:
                        req['all'].append(k)
                r_any = any([val.get(k) for k in req['any']])
                r_all = all([val.get(k) for k in req['all']])
                _msgs = []
                if r_all is False:
                    _msgs.append('required keys %s' % ', '.join([
                        k for k in req['all'] if val.get(k) is None
                    ]))
                if len(req['any']) > 0 and r_any is False:
                    _msgs.append('any of keys %s' % ', '.join(req['any']))
                if len(_msgs) > 0:
                    raise ConfigException(
                        '%s missing %s' % (msg, ' and '.join(_msgs))
                    )
            return val

        ops = []
        if isinstance(data, list):
            for i in range(len(data))[::-1 if reverse_list else None]:
                ops.extend(self.get_deploy_ops(
                    data[i], data, i, config_dir, reverse_list
                ))

        if not isinstance(data, dict):
            return ops
        for k in data.keys():
            if isinstance(data[k], dict):
                ops.extend(self.get_deploy_ops(
                    data[k], data, k, config_dir, reverse_list
                ))
            elif isinstance(data[k], list):
                for i in range(len(data[k]))[::-1 if reverse_list else None]:
                    ops.extend(self.get_deploy_ops(
                        data[k][i], data[k], i,
                        config_dir, reverse_list
                    ))

            # interactive prompt (not useful for non-interactive gitops!)
            if isinstance(data[k], str) and data[k].startswith('!prompt:'):
                data[k] = prompt(data[k])

            # serialise value as json
            if k == '$json':
                pd[pk] = json.dumps(data[k])
            # serialise list of values as json
            elif k == '$dataset':
                _validate_config_var(k, data[k], list)
                pd[pk] = json.dumps([
                    {'name': v, 'value': v} for v in data[k]
                ])
            # load csv file as json
            elif k == '$datasetCsv':
                csv_file = _validate_config_var(k, data[k], 'file', ext='csv')
                _csv_data = []
                with open(csv_file, 'r') as fh:
                    reader = csv.DictReader(fh)
                    for row in reader:
                        _csv_data.append(row)
                pd[pk] = json.dumps(_csv_data)
            # load text from any file
            elif k.startswith('$fileContent'):
                content_file = _validate_config_var(k, data[k], 'file')
                pd[pk] = open(content_file, 'r').read()
            elif k == '$id':
                pd[pk] = _validate_config_var(k,  data[k], '$id')
            elif k == '$deleteIds':
                data[k] = _validate_config_var(k, data[k], '$ids')
                for var in data[k]:
                    (path, name) = var[5:-1].split(':')
                    ops.append({
                        'operation': 'deleteIds',
                        'path': path,
                        'name': name,
                        'data': {
                            'name': name
                        }
                    })

            # upsert/delete API
            elif k.startswith('$') and k not in NON_API_VARIABLES:
                path = k[1:]
                _validate_config_var(k, data[k], dict, keys=['|name', '|id'])
                if not path.startswith('/'):
                    if path[-1] != 's':
                        path += 's'
                api_path = get_api_path(path)
                name = data[k].get('name')
                required_attrs = REQUIRED_ATTRIBUTES.get(api_path)
                if required_attrs is not None:
                    _validate_config_var(k, data[k], dict, keys=required_attrs)
                if name is not None and pd is not None and pk is not None:
                    pd[pk] = '${id:%s:%s}' % (api_path, data[k]['name'])
                op = {
                    'createPath': data[k].pop('$createPath', None),
                    'updatePath': data[k].pop('$updatePath', None),
                    'deletePath': data[k].pop('$deletePath', None),
                    'entity': data[k].pop('$entity', None),
                    'entityId': data[k].pop('$entityId', None),
                    'setName': data[k].pop('$setName', True),
                }
                op.update({
                    'path': api_path,
                    'data': data[k],
                    'name': data[k].get('name'),
                    'id': data[k].get('id')
                })
                ops.append(op)

        return ops

    def deploy(self, data, undeploy=False, config_dir=None):
        """Deploy/Undeploy nested entities

        :param data: nested dict
        :type data: dict
        :key config_dir: str of path to config dir
        :key reverse_list: bool
        :return: list of operations
        :rtype: list
        """
        ops = self.get_deploy_ops(
            data, config_dir=config_dir, reverse_list=undeploy
        )
        if undeploy is True:
            ops.reverse()

        path_names = {}

        def _add_path_name(path, name, id):
            path_name = '%s:%s' % (get_api_path(path), name or id)
            if id is not None:
                path_names[path_name] = id

        for op in ops:
            data = op.pop('data', None) if undeploy is True else op['data']
            op = self.expand_str(op)
            op['data'] = data
            path = op['path']
            name = op.get('name')
            entity = op.get('entity')
            entity_id = op.get('entityId', op.get('id'))
            operation = op.get('operation')
            delete_path = op.get('deletePath') or path
            if operation in ['deleteIds']:
                if name[-1] == '*':
                    name_ids = self.get_name_ids(
                        path, starts_with=name[:-1]
                    )
                    for _name, _id in name_ids.items():
                        _add_path_name(path, _name, self.delete(
                            delete_path, None, entity_id=_id, force=True
                        ))
                else:
                    _add_path_name(
                        path, name, self.delete(
                            delete_path, name, entity_id=entity_id, force=True
                        )
                    )
            else:
                if undeploy is True:
                    _add_path_name(
                        path, name, self.delete(
                            delete_path, name,
                            entity=entity, entity_id=entity_id, force=True
                        )
                    )
                else:
                    _add_path_name(
                        path, name, self.upsert(
                            path, name, op['data'],
                            entity,
                            entity_id=entity_id,
                            set_name=op.get('setName'),
                            create_path=op.get('createPath'),
                            update_path=op.get('updatePath')
                        )
                    )
        return path_names

    def deploy_files(self, objects, undeploy=False):
        """Deploy nested data file(s) or dirs

        :param objects: list of files and/or directories containing yaml files
        :type objects: list
        :return: list of upserted entity/object IDs
        :rtype: list
        """
        if not isinstance(objects, list):
            objects = [objects]
        files = []
        for o in objects:
            if os.path.isdir(o):
                files.extend(glob.glob(os.path.join(o, '*.yaml')))
            elif os.path.isfile(o) and o.endswith('.yaml'):
                files.append(o)
        files = sorted(files)
        fc = 0
        tc = len(files)
        path_names = []
        operation = 'deployed'
        if undeploy is True:
            operation = 'undeployed'
        for file in files:
            fc += 1
            fname = os.path.basename(file)
            with open(file, 'r') as fh:
                data = yaml.safe_load(fh.read())
                path_names.extend(self.deploy(
                    data, undeploy=undeploy, config_dir=os.path.dirname(file),
                ))
            self.print_msg('%s/%s] %s %s' % (
                fc, tc,
                operation,
                COLOR.blue(fname) if self.colorize else fname
            ))
        self.print_msg('%s %s/%s file(s)' % (
            operation, fc, tc
        ))
        return path_names


def console_main(_args=None, _fatal_handler=None):
    args = _args or sys.argv[1:]
    parser = argparse.ArgumentParser(
        description='Query Morpheus data API and deploy config to it'
    )
    parser.add_argument(
        'operation', choices=['deploy', 'undeploy', 'get', 'export']
    )
    parser.add_argument(
        'path', help='yaml file, dir of yaml files or api path'
    )
    parser.add_argument(
        '--name', help='export name'
    )
    parser.add_argument(
        '-q', help='jmespath query'
    )
    parser.add_argument(
        '-y', help='output in yaml format', action='store_true'
    )
    args = parser.parse_args(args)
    required = ['MORPHEUS_HOST', 'MORPHEUS_TOKEN']
    missing = [k for k in required if k not in os.environ]
    _fatal_handler = _fatal_handler or fatal_handler
    if len(missing) > 0:
        _fatal_handler('missing env vars: %s' % ', '.join(missing))
        return
    api = MorpheusDataApi(fatal_handler=_fatal_handler)
    if args.operation == 'get':
        r = api.call(args.path, transform=args.q)
        api.print_handler(yaml.dump(r) if args.y else json.dumps(r, indent=2))
    elif args.operation == 'export':
        if args.name is None:
            _fatal_handler('--name required')
            return
        txt = yaml.dump({
            '$' + api.get_entity_from_path(args.path, single=True): (
                api.get(args.path, args.name)
            )
        })
        api.print_handler(txt)
    else:
        api.deploy_files(
            args.path, undeploy=args.operation == 'undeploy'
        )
