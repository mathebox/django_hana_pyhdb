Django DB Backend for SAP HANA
----------------------------
Under Development

Log
------

Need to make a decorator for creating column tables out of models

Select and update works with tested models. Placeholder conversion works!!

Major problem is the paramstyle, hdb dbapi supports qmark while django assumes %s, need to write a converter

Hana doesn't support timestamp with timezone info, stripping timezone info from the string

Simple inserts work

syncdb creates tables, sequences and indexes

connects to db

uses the settings_dict['NAME'] as default schema

Disclaimer
--------------
This project is not a part of standard SAP HANA delivery, hence SAP support is not responsible for any queries related to
this software. All queries/issues should be reported here.
