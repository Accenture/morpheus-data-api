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
import jmespath
import json
import os
import re
import responses
from urllib import parse as urlparse

from morpheus_data_api import get_api_path
from morpheus_data_api import get_entity_from_path

NESTED_ENTITIES = ['job', 'blueprint', 'cypher']


def _expand_method_paths(d):
    if not isinstance(d, dict):
        return {}
    expanded = {}
    for k, v in d.items():
        parts = k.split(':')
        if len(parts) == 2:
            k = ':'.join([parts[0], get_api_path(parts[1])])
            expanded[k] = v
    return expanded


# this is here so can mock morpheus api from other module tests
class MockMorpheusDataApi:

    def __init__(
        self, host=None, debug=False, data=None, validators=None,
        transforms=None, reset=False, put_not_found=False
    ):
        """Morpheus Data API Mock Class

        :key host: host pattern to match, default will look for /api/
        :key debug: debug mock requests if True
        :key data: data file or dict to initialise with
        :key validators: dict of method:entitypath = functions
        :key transforms: dict of method:entitypath = functions
        :return: None
        :rtype: None
        """
        if reset is True:
            responses.reset()
        self.data = {}
        if isinstance(data, str) and os.path.isfile(data):
            self.load_data(data)
        elif isinstance(data, dict):
            self.data = data
        self._debug = debug
        self.validators = _expand_method_paths(validators)
        self.transforms = _expand_method_paths(transforms)
        self.put_not_found = None if put_not_found is True else 'PUT'

        url_pattern = re.compile('^https://%s.*/api/.*$' % (
            host or os.environ.get('MORPHEUS_HOST', '')
        ))
        for m in [
            responses.GET, responses.POST, responses.PUT, responses.DELETE,
            responses.PATCH
        ]:
            responses.add_callback(
                m,
                url_pattern,
                callback=self.request_handler,
                content_type='application/json'
            )

    def debug(self, msg):
        if self._debug is True:
            print('DEBUG: %s' % msg)

    def save_data(self, file):
        with open(file, 'w') as fh:
            fh.write(json.dumps(self.data))

    def load_data(self, file):
        with open(file, 'r') as fh:
            self.data = json.loads(fh.read())

    def request_handler(self, request):
        parsed = urlparse.urlsplit(request.path_url)
        params = dict(urlparse.parse_qsl(parsed.query))
        path = parsed.path
        method = request.method.upper()
        body = request.body
        self.debug('MOCK REQUEST %s %s %s' % (method, path, params))
        if request.body is not None:
            self.debug('MOCK BODY: %s' % body)
        parts = path.split('/')
        entity_path = path
        entity_id = None
        if re.match('^[0-9]+$', parts[-1]):
            entity_path = '/'.join(parts[0:-1])
            entity_id = parts[-1]
        if isinstance(body, str):
            body = json.loads(body)
        if path.startswith('/api/cypher'):
            parts = path.split('/')
            if len(parts) > 3:
                entity_path = '/'.join(parts[0:3])
                entity_id = '/'.join(parts[3:])

        code = 400
        response = None

        def _upsert_data(entity_id):
            entity_data = {}
            entity = get_entity_from_path(entity_path, single=True)
            if len(body.keys()) > 0:
                entity = list(body.keys())[0]
            if entity in body:
                entity_data = body[entity]
            if entity_path not in self.data:
                self.data[entity_path] = {
                    'counter': 0,
                    'entity': entity,
                    'data': {}
                }
            if entity_id is None:
                self.data[entity_path]['counter'] += 1
                entity_id = str(self.data[entity_path]['counter'])
            entity_data['id'] = entity_id
            self.data[entity_path]['data'][entity_id] = entity_data
            return (entity, entity_id, entity_data)

        # custom validators if needed
        validator_key = method + ':' + entity_path
        if validator_key in self.validators:
            response = self.validators[validator_key](entity_id, body)
            if response is not None:
                if isinstance(response, str):
                    response = {'msg': response}
                return (400, {}, json.dumps(response))

        if entity_id is not None:
            entity_d = self.data.get(
                entity_path, {}
            ).get('data', {}).get(entity_id)
            if entity_d is None and method != self.put_not_found:
                code = 404
            elif method == 'GET':
                entity = self.data[entity_path]['entity']
                response = {
                    entity: entity_d
                }
                code = 200
            elif method == 'DELETE':
                response = None
                self.data[entity_path]['data'].pop(entity_id, None)
                code = 200
            elif method == 'PUT':
                (entity, entity_id, entity_data) = _upsert_data(entity_id)
                response = {
                    'id': entity_id
                }
                code = 200
            else:
                code = 400
        else:
            if method == 'POST':
                # convert /api/library/instance-types/:id/layouts
                # to      /api/library/layouts
                if len(parts) == 6:
                    entity_path = '/'.join(parts[0:3] + [parts[-1]])
                (entity, entity_id, entity_data) = _upsert_data(entity_id)
                response = {
                    'id': entity_id
                }
                code = 200
                if entity in NESTED_ENTITIES:
                    response = {
                        'success': True,
                        entity: response
                    }
            elif method == 'GET':
                data = []
                entity = None
                if entity_path in self.data:
                    entity = self.data[entity_path]['entity'] + 's'
                    for d in self.data[entity_path]['data'].values():
                        name = params.get('name')
                        if name is not None and d.get('name') != name:
                            continue
                        data.append(d)
                response = {
                    entity or get_entity_from_path(path): data
                }
                code = 200
            else:
                code = 400

        if code == 404:
            response = {'msg': 'not found'}
        elif code == 400:
            response = {'msg': 'bad request'}
        if path == '/api/servererror':
            code = 500
            response = 'internal server error'

        # custom transformers if needed
        transform_key = method + ':' + entity_path
        if code in [200, 201] and transform_key in self.transforms:
            transform = self.transforms[transform_key]
            if isinstance(transform, str):
                response = jmespath.search(transform, response)
            else:
                response = transform(entity_id, response)

        self.debug('MOCK RESPONSE %s %s' % (code, response))
        return (code, {}, response if code == 500 else json.dumps(response))
