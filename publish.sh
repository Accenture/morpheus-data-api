#!/bin/bash
pytest --cov=morpheus-data-api tests/ --cov-report html:/tmp/htmlcov --cov-fail-under 95
flake8 .
python setup.py sdist bdist_wheel
twine upload dist/morpheus_data_api-0.0.2-py2.py3-none-any.whl