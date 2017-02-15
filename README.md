# Django DB Backend for SAP HANA

[![Build Status](https://travis-ci.org/mathebox/django_hana_pyhdb.svg?branch=master)](https://travis-ci.org/mathebox/django_hana_pyhdb)
[![codecov](https://codecov.io/gh/mathebox/django_hana_pyhdb/branch/master/graph/badge.svg)](https://codecov.io/gh/mathebox/django_hana_pyhdb)

- build on top of [PyHDB](https://github.com/SAP/PyHDB)
- original work done by [@kapilratnani](https://github.com/kapilratnani) (https://github.com/kapilratnani/django_hana)

## Installation
1. Install [PyHDB](https://github.com/SAP/PyHDB)

1. Install the python package via setup.py

    ```bash
	python setup.py install
	```

1. The config in the Django project is as follows

	```python
	DATABASES = {
        'default': {
            'ENGINE': 'django_hana',           # or as per your python path
            'NAME': '<SCHEMA_NAME>',           # The schema to use. It will be created if doesn't exist
            'USER': '<USERNAME>',
            'PASSWORD': '<PASSWORD>',
            'HOST': '<HOSTNAME>',
            'PORT': '3<INSTANCE_NUMBER>15',
        }
    }
    ```
1. HANA doesn't support Timezone. Set USE_TZ=False in settings.py.

## Config
### Column/Row store
Use the column/row-store class decorators to make sure that your models are using the correct HANA engine. If the models are not using any decorators the default behaviour will be a ROW-store column.
```python
from django.db import models
from django_hana import column_store, row_store

@column_store
class ColumnStoreModel(models.Model):
	some_field = models.CharField()

@row_store
class RowStoreModel(models.Model):
	some_field = models.CharField()
```

### Support of spatial column types
Add `django.contrib.gis` to your `INSTALLED_APPS`.

In your `models.py` files use
```
from django.contrib.gis.db.models import ...
```
instead of
```
from django.db.models import ...
```
Make use of the following fields:
- PointField
- LineStringField
- PolygonField
- MultiPointField
- MulitLineString
- MultiPolygon

## Contributing

1. Fork repo
1. Create your feature branch (e.g. `git checkout -b my-new-feature`)
1. Implement your feature
1. Commit your changes (e.g. `git commit -am 'Add some feature'` | See [here](https://git-scm.com/book/ch5-2.html#_commit_guidelines)) 
1. Push to the branch (e.g. `git push -u origin my-new-feature`)
1. Create new pull request

## Setting up for developement / Implement a feature

1. (Optional) Create virtualenv
1. Install development dependencies (`pip install -r requirements-testing.txt`)
1. Add test case
1. Run tests
  1. For all supported python and django version: `tox`
  1. For a single env: `tox -e <ENVNAME>` (e.g. `tox -e py35django110`)
  1. Tests failing?
1. Hack, hack, hack
1. Run tests again
  1. Tests should pass
1. Run isort (`isort -rc .` or `tox -e isort`)
1. run flake8 (`flake8 .` or `tox -e lint`)


## Disclaimer
This project is not a part of standard SAP HANA delivery, hence SAP support is not responsible for any queries related to
this software. All queries/issues should be reported here.
