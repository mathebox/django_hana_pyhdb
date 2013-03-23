Django DB Backend for SAP HANA
==============================
Under Development

Prerequisite
------------
1. Python compiled with --enable-unicode=ucs2 or use the python provided with hdbclient package.
2. Django 1.4 (Only tested with this version, 1.5 might work)
3. HANA python driver configured in python path. Only available for windows x86_64 and linux x86_64
4. Tested with HDB version 1.00.45.371235 (NewDB100_REL). 

Setup
------
1. copy all files in django/db/backends/hana directory or anywhere you want, making sure it is in python path
2. The config in the Django project is as follows
		
		DATABASES = {
		    'default': {
		        'ENGINE': 'django.db.backends.hana', # or as per your python path
		        'NAME': '<SCHEMA_NAME>',                      # The schema to use. It will be created if doesn't exist
		        'USER': '<USERNAME>',
		        'PASSWORD': '<PASSWORD>',
		        'HOST': '<HOSTNAME>',                      
		        'PORT': '3<INSTANCE_NUMBER>15',               
		    }
		}


Log
------
-	HANA doesn't return id after insert. Currently taking the curval of the sequence after insert. This may cause problems when too many inserts are done simultaneously. Needs rework. 
	May be grab the seq's nextval while constructing insert query and use it in place of id and return it after insert is done.
-	Tested with models in official django tutorial and Models References. All queries worked. 
-	Currently, executes set schema on every cursor creation. Prefixing each table name with schema name is more efficient. Needs rework.
-	connects to db
-	uses the settings_dict['NAME'] as default schema
-	syncdb creates tables, sequences and indexes
-	Simple inserts work
-	Major problem is the paramstyle hdb supports qmark while django assumes %s, need to write a converter
-	Hana doesn't support timestamp with timezone info, stripping timezone info from the string
-	Select and update works with tested models

TODO
-----
-	Create decorator for creating column store tables. Currently, all tables are created as row stores.
-	Make a custom auto field for UUID
-	Do more tests

Disclaimer
--------------
This project is not a part of standard SAP HANA delivery, hence SAP support is not responsible for any queries related to
this software. All queries/issues should be reported here.
