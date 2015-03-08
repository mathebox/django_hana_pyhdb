from __future__ import unicode_literals

from django.db.backends import BaseDatabaseIntrospection


class DatabaseIntrospection(BaseDatabaseIntrospection):
    # Maps type codes to Django Field types.
    data_types_reverse = {
        16: 'BooleanField',
        20: 'BigIntegerField',
        21: 'SmallIntegerField',
        3: 'IntegerField',
        25: 'TextField',
        700: 'FloatField',
        701: 'FloatField',
        869: 'GenericIPAddressField',
        9: 'CharField',
        1082: 'DateField',
        1083: 'TimeField',
        16: 'DateTimeField',
        1266: 'TimeField',
        1700: 'DecimalField',
    }

    def get_table_list(self, cursor):
        "Returns a list of table names in the current database."
        cursor.execute("select table_name from tables where schema_name='%s'" % self.connection.default_schema)
        result = [row[0].lower() for row in cursor.fetchall()]
        return result

    def table_name_converter(self, name):
        return unicode(name.upper())

    def get_table_description(self, cursor, table_name):
        "Returns a description of the table, with the DB-API cursor.description interface."
        cursor.execute("SELECT * FROM %s LIMIT 1" % self.connection.ops.quote_name(table_name))
        return cursor.description
