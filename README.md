# morpheus-data-client
Python client to Morpheus Data API

https://apidocs.morpheusdata.com/

![Tests](https://github.com/Accenture/morpheus-data-api/actions/workflows/tests.yml/badge.svg)
![Codecov](https://codecov.io/gh/Accenture/morpheus-data-api/branch/master/graph/badge.svg)

## Usage ##

```python
import os
from morpheus_data_api import MorpheusDataApi

os.environ['MORPHEUS_HOST'] = 'somehost.com'
os.environ['MORPHEUS_TOKEN'] = 'foobar'

api = MorpheusDataApi()
type_data = {
    'optionType': {
        'name': 'foo',
        'type': 'text',
        'description': 'foo'
    }
}

def type_names():
    r = api.call(
        'library/option-types', transform='optionTypes[].name'
    )
    return r

assert 'foo' not in type_names()

api.upsert('library/option-types', 'foo', type_data)
assert 'foo' in type_names()

type_data['optionType']['description'] = 'FOO'
assert api.get('library/option-types', 'foo')['description'] == 'foo'

api.upsert('library/option-types', 'foo', type_data)
assert api.get('library/option-types', 'foo')['description'] == 'FOO'

api.delete('library/option-types', 'foo')
assert 'foo' not in type_names()

```

### Deploy/Undeploy Config ###

Given a yaml config file [tests/data/option_types/foo1.yaml](./tests/data/option_types/foo1.yaml):

```yaml
$optionType:
  name: foo1
  type: select
  description: foo1
  optionList:
    id:
      $optionTypeList:
        name: foo1
        type: manual
        initialDataset:
          $dataset:
            - bar
            - baz
```

It can be deployed/undeployed like so:

```python
import os
from morpheus_data_api import MorpheusDataApi

os.environ['MORPHEUS_HOST'] = 'somehost.com'
os.environ['MORPHEUS_TOKEN'] = 'foobar'

api = MorpheusDataApi()
api.deploy_files('option_types/foo1.yaml')
api.deploy_files('option_types/foo1.yaml', undeploy=True)
```

Or deployed through the `morpheus-data-api` console script

```console
$ morpheus-data-api deploy tests/data/option_types/foo1.yaml
created optionTypeList foo1 [14]
created optionType foo1 [1766]
1/1] deployed foo1.yaml
deployed 1/1 file(s)
```

Then undeployed:
```console
$ morpheus-data-api undeploy tests/data/option_types/foo1.yaml
deleted optionType foo1 [1766]
deleted optionTypeList foo1 [14]
1/1] undeployed foo1.yaml
undeployed 1/1 file(s)
```

This works by upserting or deleting nested entities in the expected order, and linking
referential IDs to the parent object

See [tests/data/catalog_items/item1.yaml](./tests/data/catalog_items/item1.yaml) for larger example

```console
$ morpheus-data-api deploy tests/data/catalog_items/item1.yaml
created task task1 [9]
created optionType item1 [1767]
created optionTypeList item2 [15]
created optionType item2 [1768]
created taskSet item1 [5]
created catalogItemType item1 [3]
created schedule schedule1 [6]
created job job1 [11]
1/1] deployed item1.yaml
deployed 1/1 file(s)
```

### Variables ###

The following variables are supported within the config file

| variable | description | example |
| --- | --- | --- |
| `$createPath` | nested below an $api object, override the path used to create entity | `$createPath: /api/library/instance-types/${id:instanceTypes:blueprint1.instanceType1}/layouts` |
| `$dataset` | convert list of values to json optionType dataset | `$dataset: ['foo', 'bar']` |
| `$datasetCsv` | convert contents of local csv file to json optionType dataset | `$datasetCsv: data.csv` |
| `$deleteIds` | delete additional entities during undeploy | `$deleteIds: [${id:optionTypes:foo}]` |
| `$deletePath` | nested below an $api object, override the path used to delete entity | `$deletePath: /api/library/instance-types/${id:instanceTypes:blueprint1.instanceType1}/layouts` |
| `$entity` | nested below an $api object, override entity name | `$entity: instanceTypeLayout` |
| `$entityId` | nested below an $api object, override entity ID | `$entityId: key/128/foobar` |
| `$fileContent` | read contents of local file | `$fileContent: foo.py` |
| `$id` | lookup entity ID from ${id:path:name} expression | `id: ${id:optionTypes:foo}` |
| `$json` | convert value to JSON | `$json: [1,2,3]` |
| `$setName` | nested below an $api object, don't automatically set entity name if `false` | `$setName: false` |
| `$updatePath` | nested below an $api object, override the path used to update entity | `$updatePath: /api/library/instance-types/${id:instanceTypes:blueprint1.instanceType1}/layouts` |
| `$validate` | nested below an $api object, disable validation if `false` | `$validate: false` |

## Console Script ##

The `morpheus-data-api` console script is installed as part of setup.py

```console
usage: morpheus-data-api [-h] [--name NAME] [-q Q] [-y]
                         {deploy,undeploy,get,export} path

Query Morpheus data API and deploy config to it

positional arguments:
  {deploy,undeploy,get,export}
  path                  yaml file, dir of yaml files or api path

optional arguments:
  -h, --help            show this help message and exit
  --name NAME           export name
  -q Q                  jmespath query
  -y                    output in yaml format
```

## MockMorpheusDataApi ##

Bundled into the package is `mock.MockMorpheusDataApi()` which provides full persistent
mocking of Morpheus API.  See [tests/test_morpheus_data_api.py](./tests/test_morpheus_data_api.py) for examples how this is used.

## License
The license is Apache 2.0, see [LICENSE](./LICENSE) for the details.