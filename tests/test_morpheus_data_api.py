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
from copy import deepcopy
import os
import pytest
import responses
import sys
from tempfile import gettempdir

ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(ROOT_DIR, 'tests', 'data')
sys.path.append(ROOT_DIR)

from morpheus_data_api import ConfigException  # noqa: E402
from morpheus_data_api import console_main  # noqa: E402
from morpheus_data_api import ConstructorException  # noqa: E402
from morpheus_data_api import HttpException  # noqa: E402
from morpheus_data_api import MorpheusDataApi  # noqa: E402
from morpheus_data_api import MorpheusDataApiException  # noqa: E402
from morpheus_data_api import NullTransformException  # noqa: E402

from morpheus_data_api.mock import MockMorpheusDataApi  # noqa: E402

TYPE_DATA = {
    'optionType': {
        'name': 'foo',
        'type': 'text',
        'description': 'foo',
        'fieldName': 'foo',
        'fieldLabel': 'foo'
    }
}
DATA_FILE = os.path.join(gettempdir(), 'morpheus-data-api-test-mock.json')


def set_env_vars():
    for k in ['MORPHEUS_HOST', 'MORPHEUS_TOKEN']:
        os.environ[k] = 'foobar'


def unset_env_vars():
    for k in ['MORPHEUS_HOST', 'MORPHEUS_TOKEN']:
        os.environ.pop(k, None)


def remove_data_file():
    if os.path.isfile(DATA_FILE):
        os.remove(DATA_FILE)


def test_constructor_exceptions():
    unset_env_vars()
    with pytest.raises(Exception) as e:
        MorpheusDataApi()
    assert e.type == ConstructorException
    assert str(e.value) == 'host, token required'
    with pytest.raises(Exception) as e:
        MorpheusDataApi(host='foo')
    assert e.type == ConstructorException
    assert str(e.value) == 'token required'
    with pytest.raises(Exception) as e:
        MorpheusDataApi(token='bar')
    assert e.type == ConstructorException
    assert str(e.value) == 'host required'
    os.environ['MORPHEUS_HOST'] = 'foo'
    with pytest.raises(Exception) as e:
        MorpheusDataApi()
    assert e.type == ConstructorException
    assert str(e.value) == 'token required'
    unset_env_vars()


def test_ssl_verify():
    set_env_vars()
    assert MorpheusDataApi().ssl_verify is True
    assert MorpheusDataApi(ssl_verify=False).ssl_verify is False
    os.environ['MORPHEUS_SSL_VERIFY'] = 'FALSE'
    assert MorpheusDataApi().ssl_verify is False


@responses.activate
def test_upsert():
    set_env_vars()
    MockMorpheusDataApi(debug=True)
    api = MorpheusDataApi()
    type_data = deepcopy(TYPE_DATA)

    type_id = api.upsert('library/option-types', 'foo', type_data)

    r = api.call('library/option-types/%s' % type_id, params={'bar': 'baz'})
    assert r['optionType']['description'] == 'foo'

    type_data['optionType']['description'] = 'FOO'
    api.upsert('library/option-types', 'foo', type_data)

    r = api.call('library/option-types/%s' % type_id)
    assert r['optionType']['description'] == 'FOO'

    # test some of the other options
    (code, d) = api.call(
        'library/option-types', data=TYPE_DATA,
        return_code=True, allowed_codes=[200]
    )
    assert code == 200
    assert d['id'] == '2'

    # test that doesnt fail when entity ID doesnt exist to delete
    api.delete('library/option-types', 'baz')
    api.delete_ids('library/option-types', '1', force=True)
    api.delete_ids('library/option-types', ['1', '2'], force=True)


@responses.activate
def test_expand_str():
    set_env_vars()
    MockMorpheusDataApi()
    api = MorpheusDataApi()
    assert api.expand_str('${foo}', {'${foo}': 'bar'}) == 'bar'
    assert api.expand_str('${foo:bar}') == '${foo:bar}'
    with pytest.raises(HttpException):
        api.expand_str('${id:foo:bar}')
    api.upsert('foo', 'bar', {})
    assert api.expand_str('${id:foo:bar}') == '1'


@responses.activate
def test_crud():
    set_env_vars()
    MockMorpheusDataApi(debug=True)
    api = MorpheusDataApi(print_msg=False)

    # don't need to specify library/ prefix for option-types
    def type_names():
        r = api.call('option-types', transform='optionTypes[].name')
        return r

    assert 'foo' not in type_names()

    # can also use mixedCase syntax
    api.upsert('optionTypes', 'foo', TYPE_DATA, set_name=False)
    assert 'foo' in type_names()
    assert api.get('optionTypes', 'foo')['description'] == 'foo'

    api.upsert('optionTypes', 'foo', {
        'type': 'text',
        'description': 'FOO'
    })
    assert api.get('optionTypes', 'foo')['description'] == 'FOO'

    api.delete('optionTypes', 'foo')
    assert 'foo' not in type_names()


@responses.activate
def test_exceptions():
    set_env_vars()
    MockMorpheusDataApi(put_not_found=True)
    api = MorpheusDataApi(print_msg=False)
    with pytest.raises(Exception) as e:
        api.call('library/option-types/999')
    assert e.type == HttpException
    assert e.value.code == 404
    assert e.value.message == 'not found'
    assert str(e.value) == 'HTTP [404] not found'

    with pytest.raises(Exception) as e:
        api.call('foo', 'delete')
    assert e.type == HttpException
    assert e.value.code == 400

    with pytest.raises(Exception) as e:
        api.call('foo/1', 'put', {})
    assert e.type == HttpException
    assert e.value.code == 404

    api.upsert('library/option-types', 'foo', TYPE_DATA)
    with pytest.raises(Exception) as e:
        api.call('library/option-types/1', 'patch', TYPE_DATA)
    assert e.type == HttpException
    assert e.value.code == 400

    with pytest.raises(Exception) as e:
        api.call('foo', transform='baz')
    assert e.type == NullTransformException

    with pytest.raises(Exception) as e:
        api.call('servererror')
    assert e.type == HttpException
    assert e.value.code == 500
    assert e.value.message == b'internal server error'


@responses.activate
def test_handlers():
    set_env_vars()
    MockMorpheusDataApi()
    prints = []

    def print_handler(msg):
        prints.append(msg)

    def _fatal_handler(msg):
        return msg

    api = MorpheusDataApi(
        print_handler=print_handler,
        fatal_handler=_fatal_handler,
        colorize=False
    )

    api.upsert('/api/library/option-types', 'foo', TYPE_DATA)
    api.upsert('library/option-types', 'bar', TYPE_DATA)
    assert prints == [
        'created optionType foo [1]',
        'created optionType bar [2]'
    ]
    api.call('library/option-types/1')
    assert api.call('library/option-types/999') == (
        'HTTP GET /api/library/option-types/999 [404]: not found'
    )


@responses.activate
def test_entity_methods():
    set_env_vars()
    MockMorpheusDataApi(debug=True, transforms={
        'GET:/api/foo/ids': "['1']"
    })
    api = MorpheusDataApi()
    assert api.get_entity_id('foo', 'bar', entity_id='123') == '123'
    assert api.upsert('foo', 'bar', {'foo': {'bar': 'baz'}}) == '1'
    assert api.get_entity_from_path('foo', entity='foos', single=True) == 'foo'
    with pytest.raises(Exception) as e:
        api.get('bar', 'baz')
    assert e.type == HttpException
    assert e.value.code == 404
    assert e.value.message == 'not found'
    assert api.call('/api/foo/ids', get_entity=True) == ['1']


@responses.activate
def test_mock_validators():
    set_env_vars()
    MockMorpheusDataApi(validators={
        'POST:/api/badrequest': lambda x, y: {'msg': 'bad request'},
        'POST:/api/library/option-types': lambda x, y: None,
        'PUT:optionTypes': lambda x, y: 'BAD REQUEST!'
    })
    api = MorpheusDataApi()
    with pytest.raises(Exception) as e:
        api.upsert('badrequest', 'foo', {})
    assert e.type == HttpException
    assert e.value.code == 400
    assert e.value.message == 'bad request'

    api.upsert('library/option-types', 'foo', TYPE_DATA)
    with pytest.raises(HttpException) as e:
        api.upsert('library/option-types', 'foo', TYPE_DATA)
    assert e.value.code == 400
    assert e.value.message == 'BAD REQUEST!'


@responses.activate
def test_mock_transforms():
    set_env_vars()
    MockMorpheusDataApi(transforms={
        'GET:/api/library/option-types': lambda id, r: (
            {'z': r} if id == '1' else r
        ),
        'POST:/api/library/option-types': "{id: @}",
        'POST': "never called",
        'POST:/api/foo':  "{bar:{ID: '99'}}"
    }, debug=True)
    api = MorpheusDataApi()
    assert api.upsert('library/option-types', 'foo', TYPE_DATA) == {'id': '1'}
    assert api.upsert('library/option-types', 'bar', TYPE_DATA) == {'id': '2'}
    assert 'optionType' in api.call('library/option-types/1')['z']
    assert 'optionType' in api.call('library/option-types/2')
    with pytest.raises(MorpheusDataApiException) as e:
        api.upsert('/api/foo', 'foo', {'foo': {'bar': '1'}})
    assert str(e.value) == 'entity id not found in POST response for /api/foo'


@responses.activate
def test_get_name_ids():
    set_env_vars()
    MockMorpheusDataApi(debug=True)
    api = MorpheusDataApi()
    assert api.upsert('library/option-types', 'foo1', TYPE_DATA)
    assert api.upsert('library/option-types', 'foo2', TYPE_DATA)
    assert api.upsert('library/option-types', 'bar1', TYPE_DATA)
    assert api.get_name_ids('library/option-types') == {
        'foo1': '1',
        'foo2': '2',
        'bar1': '3'
    }
    assert api.get_name_ids('library/option-types', starts_with='foo') == {
        'foo1': '1',
        'foo2': '2'
    }


@responses.activate
def test_mock_save_data():
    set_env_vars()
    remove_data_file()
    m1 = MockMorpheusDataApi(data={})
    api = MorpheusDataApi()
    api.upsert('library/option-types', 'foo', TYPE_DATA)
    m1.save_data(DATA_FILE)
    MockMorpheusDataApi(data=DATA_FILE, reset=True)
    assert api.get('library/option-types', 'foo')['description'] == 'foo'
    remove_data_file()


@responses.activate
def test_deploy():
    set_env_vars()
    MockMorpheusDataApi(debug=True)
    os.environ['MORPHEUS_DEBUG'] = 'TRUE'
    api = MorpheusDataApi()
    api.deploy({
        '$optionTypes': {
            'name': 'foo',
            'type': 'select',
            'description': 'foo',
            'fieldName': 'foo',
            'fieldLabel': 'foo',
            'optionList': {
                'id': {
                    '$optionTypeLists': {
                        'name': 'foo',
                        'type': 'manual',
                        'initialDataset': {
                            '$json': [
                                {
                                    'name': 'bar',
                                    'value': 'baz'
                                }
                            ]
                        }
                    }
                }
            }
        }
    })
    assert api.get('optionTypes', 'foo')['optionList'] == {'id': '1'}
    ld = api.call('optionTypeLists/1')['optionTypeList']
    assert ld['type'] == 'manual'
    assert ld['initialDataset'] == (
        '[{"name": "bar", "value": "baz"}]'
    )
    os.environ.pop('MORPHEUS_DEBUG', None)


@responses.activate
def test_deploy_prompt(monkeypatch):
    set_env_vars()
    MockMorpheusDataApi(debug=True)
    api = MorpheusDataApi()

    def _test(prompt, inputs, expected):
        monkeypatch.setattr('builtins.input', lambda _: inputs.pop(0))
        assert api.deploy({
            '$optionType': {
                'name': 'foo',
                'type': 'text',
                'description': prompt,
                'fieldLabel': 'foo',
                'fieldName': 'foo'
            }
        }) == {'/api/library/option-types:foo': '1'}
        assert api.get('option-types', 'foo')['description'] == expected

    _test('notprompted', ['foo'], 'notprompted')
    _test('!prompt:enter val', ['foo'], 'foo')
    _test('!prompt:enter val!pattern:FOO', ['foo', 'FOO'], 'FOO')


@responses.activate
def test_deploy_config_exceptions():
    set_env_vars()
    MockMorpheusDataApi(debug=True)
    api = MorpheusDataApi()

    def _test(k, v, msg, dir=None):
        with pytest.raises(ConfigException) as e:
            api.deploy({'x': {k: v}}, config_dir=dir)
        assert str(e.value) == 'x.%s %s' % (k, msg)

    config_dir = os.path.join(DATA_DIR, 'option_types')
    _test(
        '$dataset', 'foo',
        'must be of type list not str'
    )
    _test(
        '$datasetCsv', None,
        'None must be csv file in same dir as yaml config'
    )
    _test(
        '$datasetCsv', 'dataset',
        'dataset must be csv file in same dir as yaml config'
    )
    _test(
        '$datasetCsv', 'dataset.csv',
        'dataset.csv must be csv file in same dir as yaml config, '
        + 'config_dir required'
    )
    _test(
        '$datasetCsv', 'dataset.csv',
        'dataset.csv must be csv file in same dir as yaml config, '
        + 'config_dir required'
    )
    _test(
        '$datasetCsv', 'missing.csv',
        'missing.csv must be csv file in same dir as yaml config',
        config_dir
    )
    id_msg = 'must of type str or list in format ${id:path:name} not xxx'
    _test('$id', 'foo', id_msg.replace('xxx', 'foo'))
    _test('$id', None, id_msg.replace('xxx', 'None'))
    _test('$id', [], id_msg.replace('xxx', '[]'))
    # test actually works (and to get coverage)
    api.deploy({'x': {'$datasetCsv': os.path.join(config_dir, 'dataset.csv')}})
    api.deploy({'x': {'$datasetCsv': 'dataset.csv'}}, config_dir=config_dir)
    _test(
        '$foo', {'x': 'y'}, 'missing any of keys name, id'
    )
    _test(
        '$optionType', {'name': 'foo'},
        'missing required keys type, fieldName, fieldLabel'
    )


@responses.activate
def test_console_main():
    unset_env_vars()
    args = ['deploy', os.path.join(DATA_DIR, 'option_types', 'foo1.yaml')]

    fatals = []
    kwargs = {'_fatal_handler': lambda x: fatals.append(x)}
    MockMorpheusDataApi(debug=True)

    with pytest.raises(SystemExit):
        console_main(args)

    console_main(args, **kwargs)
    assert fatals[-1] == 'missing env vars: MORPHEUS_HOST, MORPHEUS_TOKEN'

    set_env_vars()
    fatals = []
    console_main(args, **kwargs)
    assert fatals == []

    console_main(
        ['get', 'optionTypes/1', '-q', 'optionType'], **kwargs
    )

    args = ['export', 'optionTypes']

    console_main(args, **kwargs)
    assert fatals[-1] == '--name required'

    console_main(args + ['--name', 'foo99'], **kwargs)
    assert fatals[-1] == 'HTTP [404] not found'

    console_main(args + ['--name', 'foo1', '-y'], **kwargs)


@responses.activate
def test_deploy_files():
    set_env_vars()
    MockMorpheusDataApi()
    prints = []

    def print_handler(msg):
        prints.append(msg)

    api = MorpheusDataApi(
        print_handler=print_handler,
        colorize=False
    )

    # deploy files from dir
    path_names = api.deploy_files(os.path.join(DATA_DIR, 'option_types'))
    assert path_names == [
        "/api/library/option-type-lists:foo1",
        "/api/library/option-types:foo1",
        "/api/library/option-type-lists:foo2",
        "/api/library/option-types:foo2",
        "/api/library/option-types:foo3"
    ]

    # deploy specific files
    path_names = api.deploy_files([
        os.path.join(DATA_DIR, 'option_types', 'foo2.yaml'),
        os.path.join(DATA_DIR, 'option_types', 'duff.txt'),
    ])
    assert path_names == [
        "/api/library/option-type-lists:foo2",
        "/api/library/option-types:foo2"
    ]

    assert prints == [
        "created optionTypeList foo1 [1]",
        "created optionType foo1 [1]",
        "1/3] deployed foo1.yaml",
        "created optionTypeList foo2 [2]",
        "created optionType foo2 [2]",
        "2/3] deployed foo2.yaml",
        "created optionType foo3 [3]",
        "3/3] deployed foo3.yaml",
        "deployed 3/3 file(s)",
        "updated optionTypeList foo2 [2]",
        "updated optionType foo2 [2]",
        "1/1] deployed foo2.yaml",
        "deployed 1/1 file(s)"
    ]

    # check exists
    assert api.call('optionTypes', transform='optionTypes[].name') == [
        'foo1', 'foo2', 'foo3'
    ]
    assert api.call('optionTypeLists', transform='optionTypeLists[].name') == [
        'foo1', 'foo2'
    ]
    # check $datasetCsv
    assert api.get('optionTypeLists', 'foo1')['initialDataset'] == (
        '[{"name": "foo", "value": "FOO"}, {"name": "bar", "value": "BAR"}]'
    )
    # check the $id lookup for foo2 option list works
    assert api.get('optionTypes', 'foo3')['optionList']['id'] == '2'

    # undeploy
    prints = []
    path_names = api.deploy_files(
        os.path.join(DATA_DIR, 'option_types'), undeploy=True
    )
    assert path_names == [
        "/api/library/option-types:foo1",
        "/api/library/option-type-lists:foo1",
        "/api/library/option-types:foo2",
        "/api/library/option-type-lists:foo2",
        "/api/library/option-types:foo3"
    ]
    assert api.call('optionTypes') == {'optionTypes': []}
    assert api.call('optionTypeLists') == {'optionTypeLists': []}
    assert prints == [
        "deleted optionType foo1 [1]",
        "deleted optionTypeList foo1 [1]",
        "1/3] undeployed foo1.yaml",
        "deleted optionType foo2 [2]",
        "deleted optionTypeList foo2 [2]",
        "2/3] undeployed foo2.yaml",
        "deleted optionType foo3 [3]",
        "3/3] undeployed foo3.yaml",
        "undeployed 3/3 file(s)"
    ]


@responses.activate
def test_deploy_catalog_item():
    set_env_vars()
    MockMorpheusDataApi(debug=False)
    prints = []

    def print_handler(msg):
        prints.append(msg)

    api = MorpheusDataApi(
        print_handler=print_handler,
        colorize=False
    )

    def create_ops():
        # add some option types to test wildcard $deleteIds op.*
        for i in [1, 2]:
            name = 'op.%s' % i
            api.upsert('optionTypes', name, {
                'name': name,
                'type': 'text',
                'description': name
            })

    create_ops()
    path_names = api.deploy_files(os.path.join(DATA_DIR, 'catalog_items'))

    assert path_names == [
        "/api/library/option-types:op.1",
        "/api/library/option-types:op.2",
        "/api/tasks:task1",
        "/api/library/option-types:item1",
        "/api/library/option-type-lists:item2",
        "/api/library/option-types:item2",
        "/api/task-sets:item1",
        "/api/catalog-item-types:item1",
        "/api/execute-schedules:schedule1",
        "/api/jobs:job1"
    ]
    assert prints == [
        'created optionType op.1 [1]',
        'created optionType op.2 [2]',
        'deleted optionType [1]',
        'deleted optionType [2]',
        'created task task1 [1]',
        'created optionType item1 [3]',
        'created optionTypeList item2 [1]',
        'created optionType item2 [4]',
        'created taskSet item1 [1]',
        'created catalogItemType item1 [1]',
        'created schedule schedule1 [1]',
        'created job job1 [1]',
        '1/1] deployed item1.yaml',
        'deployed 1/1 file(s)',
    ]

    # check references
    d = api.get('task-sets', 'item1')
    task_set_id = d['id']
    assert task_set_id == '1'
    assert d['tasks'] == [{'taskId': '1'}]
    assert d['optionTypes'] == ['3', '4']
    assert api.get('optionTypes', 'item2')['optionList']['id'] == '1'
    schedule_id = api.get('execute-schedules', 'schedule1')['id']
    assert api.get('jobs', 'job1')['scheduleMode'] == schedule_id

    d = api.get('catalog-item-types', 'item1')
    assert d['workflow']['id'] == task_set_id

    d = api.get('jobs', 'job1')
    assert d['workflow']['id'] == task_set_id

    prints = []

    # undeploy
    create_ops()
    prints = []
    path_names = api.deploy_files(
        os.path.join(DATA_DIR, 'catalog_items'), True
    )
    # reverse order
    assert path_names == [
        "/api/library/option-types:op.1",
        "/api/library/option-types:op.2",
        "/api/catalog-item-types:item1",
        "/api/task-sets:item1",
        "/api/library/option-types:item1",
        "/api/library/option-types:item2",
        "/api/library/option-type-lists:item2",
        "/api/tasks:task1",
        "/api/jobs:job1",
        "/api/execute-schedules:schedule1"
    ]
    # check all deleted
    for path_name in path_names:
        (path, name) = path_name.split(':')
        with pytest.raises(HttpException):
            api.get(path, name)

    assert prints == [
        "deleted optionType [5]",
        "deleted optionType [6]",
        "deleted catalogItemType item1 [1]",
        "deleted taskSet item1 [1]",
        "deleted optionType item1 [3]",
        "deleted optionType item2 [4]",
        "deleted optionTypeList item2 [1]",
        "deleted task task1 [1]",
        "deleted job job1 [1]",
        "deleted schedule schedule1 [1]",
        "1/1] undeployed item1.yaml",
        "undeployed 1/1 file(s)"
    ]


@responses.activate
def test_deploy_blueprint():
    set_env_vars()
    MockMorpheusDataApi(debug=True)
    api = MorpheusDataApi()
    file = os.path.join(DATA_DIR, 'blueprints', 'blueprint1.yaml')
    path_names = api.deploy_files(file)
    assert path_names == [
        "/api/cypher:key/128/blueprint1_key",
        "/api/library/instance-types:blueprint1_instancetype1",
        '/api/library/spec-templates:blueprint1_spec1',
        "/api/library/layouts:blueprint1_layout1",
        "/api/blueprints:blueprint1"
    ]
    # this is clearly not the correct mocked response...
    assert api.call('/api/cypher') == {
        "cyphers": [{"id": "key/128/blueprint1_key"}]
    }
    path_names = api.deploy_files(file, True)
    assert path_names == [
        "/api/blueprints:blueprint1",
        "/api/library/layouts:blueprint1_layout1",
        '/api/library/spec-templates:blueprint1_spec1',
        "/api/library/instance-types:blueprint1_instancetype1",
        "/api/cypher:key/128/blueprint1_key"
    ]
