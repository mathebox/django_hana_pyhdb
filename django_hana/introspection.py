from __future__ import unicode_literals

from django.db.backends.base.introspection import BaseDatabaseIntrospection, TableInfo


class DatabaseIntrospection(BaseDatabaseIntrospection):
    # Maps type codes to Django Field types.
    data_types_reverse = {
        # 16: 'BooleanField',
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
        """
        Returns a list of table names in the current database.
        """
        sql = (
            'select table_name, "t" from tables where schema_name="{0}" '
            'UNION select view_name, "v" from views where schema_name="{0}"'
        )
        cursor.execute(sql.format(self.connection.default_schema))
        result = [TableInfo(row[0], row[1]) for row in cursor.fetchall()]
        result = result + [TableInfo(t.name.lower(), t.type) for t in result]
        return result

    def table_name_converter(self, name):
        return unicode(name.upper())

    def get_table_description(self, cursor, table_name):
        """
        Returns a description of the table, with the DB-API cursor.description interface.
        """
        cursor.execute('SELECT * FROM %s LIMIT 1' % self.connection.ops.quote_name(table_name))
        return cursor.description

    def get_relations(self, cursor, table_name):
        """
        Returns a dictionary of {field_name: (field_name_other_table, other_table)}
        representing all relationships to the given table.
        """
        constraints = self.get_key_columns(cursor, table_name)
        relations = {}
        for my_fieldname, other_table, other_field in constraints:
            relations[my_fieldname] = (other_field, other_table)
        return relations

    def get_key_columns(self, cursor, table_name):
        """
        Returns a list of (column_name, referenced_table_name, referenced_column_name) for all
        key columns in given table.
        """
        key_columns = []
        table_name = self.connection.ops.quote_name(table_name).replace('"', '')
        schema_name = self.connection.ops.quote_name(self.connection.default_schema).replace('"', '')
        sql = (
            'SELECT column_name, referenced_table_name, referenced_column_name '
            'FROM REFERENTIAL_CONSTRAINTS '
            'WHERE table_name = %s '
            'AND schema_name = %s '
            'AND referenced_schema_name = %s '
            'AND referenced_table_name IS NOT NULL '
            'AND referenced_column_name IS NOT NULL'
        )
        cursor.execute(sql, [table_name, schema_name, schema_name])
        key_columns.extend(cursor.fetchall())
        return key_columns

    def get_constraints(self, cursor, table_name):
        constraints = {}
        table_name = self.connection.ops.quote_name(table_name).replace('"', '')
        schema_name = self.connection.ops.quote_name(self.connection.default_schema).replace('"', '')
        # Fetch pk and unique constraints
        sql = (
            'SELECT constraint_name, column_name, is_primary_key, is_unique_key '
            'FROM CONSTRAINTS '
            'WHERE schema_name = %s '
            'AND table_name = %s'
        )
        cursor.execute(sql, [schema_name, table_name])
        for constraint, column, pk, unique in cursor.fetchall():
            # If we're the first column, make the record
            if constraint not in constraints:
                constraints[constraint] = {
                    'columns': set(),
                    'primary_key': bool(pk),
                    'unique': bool(unique),
                    'foreign_key': None,
                    'check': False,  # check constraints are not supported in SAP HANA
                    'index': True,  # All P and U come with index
                }
            # Record the details
            constraints[constraint]['columns'].add(column)
        # Fetch fk constraints
        sql = (
            'SELECT constraint_name, column_name, referenced_table_name, referenced_column_name '
            'FROM REFERENTIAL_CONSTRAINTS '
            'WHERE table_name = %s '
            'AND schema_name = %s '
            'AND referenced_schema_name = %s '
            'AND referenced_table_name IS NOT NULL '
            'AND referenced_column_name IS NOT NULL'
        )
        cursor.execute(sql, [table_name, schema_name, schema_name])
        for constraint, column, ref_table, ref_column in cursor.fetchall():
            if constraint not in constraints:
                # If we're the first column, make the record
                constraints[constraint] = {
                    'columns': set(),
                    'primary_key': False,
                    'unique': False,
                    'index': False,
                    'check': False,
                    'foreign_key': (ref_table, ref_column) if ref_column else None,
                }
            # Record the details
            constraints[constraint]['columns'].add(column)
        # Fetch indexes
        sql = (
            'SELECT index_name, column_name '
            'FROM index_columns '
            'WHERE schema_name = %s '
            'AND table_name = %s'
        )
        cursor.execute(sql, [schema_name, table_name])
        for constraint, column in cursor.fetchall():
            # If we're the first column, make the record
            if constraint not in constraints:
                constraints[constraint] = {
                    'columns': set(),
                    'primary_key': False,
                    'unique': False,
                    'foreign_key': None,
                    'check': False,
                    'index': True,
                }
            # Record the details
            constraints[constraint]['columns'].add(column)
        return constraints

    def get_indexes(self, cursor, table_name):
        sql = (
            'SELECT '
            'idx_col.column_name as column_name, '
            'CASE WHEN indexes.constraint = "PRIMARY KEY" THEN 1 ELSE 0 END as is_primary_key, '
            'SIGN(LOCATE(indexes.index_type, "UNIQUE")) as is_unique '
            'FROM index_columns idx_col '
            'JOIN (SELECT index_oid '
            'FROM index_columns '
            'WHERE schema_name = %s '
            'AND table_name = %s '
            'GROUP BY index_oid '
            'HAVING count(*) = 1) single_idx_col '
            'ON idx_col.index_oid = single_idx_col.index_oid '
            'JOIN indexes indexes '
            'ON idx_col.index_oid = indexes.index_oid'
        )
        table_name = self.connection.ops.quote_name(table_name).replace('"', '')
        schema_name = self.connection.ops.quote_name(self.connection.default_schema).replace('"', '')
        cursor.execute(sql, [schema_name, table_name])
        indexes = {}
        for row in cursor.fetchall():
            indexes[row[0]] = {
                'primary_key': bool(row[1]),
                'unique': bool(row[2]),
            }
        return indexes
