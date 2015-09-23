# Django DB Backend for SAP HANA
- for Linux, Mac OS and Windows
- build on top of [PyHDB](https://github.com/SAP/PyHDB)
- original work done by [@kapilratnani](https://github.com/kapilratnani) (https://github.com/kapilratnani/django_hana)

## Setup
1. install the python package via setup.py

    ```bash
	python setup.py install
	```
   **When you use this package in a requirements file, you have to use** 
   ```bash
   pip install --process-dependency-links -r ./requirements.txt
   ```
   **in order to install all dependencies.**

2. The config in the Django project is as follows

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
3. HANA doesn't support Timezone. Set USE_TZ=False in settings.py.

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

## Test system
- Django 1.8
- HDB version 1.00.094.00.1427101515

## Disclaimer
This project is not a part of standard SAP HANA delivery, hence SAP support is not responsible for any queries related to
this software. All queries/issues should be reported here.
