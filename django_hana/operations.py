from __future__ import unicode_literals

import uuid

from django.contrib.gis.db.backends.base.adapter import WKTAdapter
from django.contrib.gis.db.backends.base.operations import BaseSpatialOperations
from django.contrib.gis.db.backends.utils import SpatialOperator
from django.contrib.gis.geometry.backend import Geometry
from django.contrib.gis.measure import Distance
from django.db.backends.base.operations import BaseDatabaseOperations
from django.utils import six
from django.utils.encoding import force_text

from .base import Database


class HanaSpatialOperator(SpatialOperator):
    sql_template = '%(lhs)s.%(func)s(%(rhs)s)'


class HanaIsOneSpatialOperator(SpatialOperator):
    sql_template = '%(lhs)s.%(func)s(%(rhs)s) = 1'


class HanaIsValueSpatialOperator(SpatialOperator):
    sql_template = '%(lhs)s.%(func)s(%(rhs)s) %(op)s %%s'


class DatabaseOperations(BaseDatabaseOperations, BaseSpatialOperations):
    compiler_module = 'django_hana.compiler'

    Adapter = WKTAdapter
    Adaptor = Adapter  # Backwards-compatibility alias.

    gis_operators = {
        'contains': HanaIsOneSpatialOperator(func='ST_CONTAINS'),
        'coveredby': HanaIsOneSpatialOperator(func='ST_COVEREDBY'),
        'covers': HanaIsOneSpatialOperator(func='ST_COVERS'),
        'crosses': HanaIsOneSpatialOperator(func='ST_CROSSES'),
        'disjoint': HanaIsOneSpatialOperator(func='ST_DISJOINT'),
        'distance': HanaIsValueSpatialOperator(func='ST_DISTANCE', op='='),
        'distance_gt': HanaIsValueSpatialOperator(func='ST_DISTANCE', op='>'),
        'distance_gte': HanaIsValueSpatialOperator(func='ST_DISTANCE', op='>='),
        'distance_lt': HanaIsValueSpatialOperator(func='ST_DISTANCE', op='<'),
        'distance_lte': HanaIsValueSpatialOperator(func='ST_DISTANCE', op='<='),
        'equals': HanaIsOneSpatialOperator(func='ST_EQUALS'),
        'exact': HanaIsOneSpatialOperator(func='ST_EQUALS'),
        'intersects': HanaIsOneSpatialOperator(func='ST_INTERSECTS'),
        'overlaps': HanaIsOneSpatialOperator(func='ST_OVERLAPS'),
        'same_as': HanaIsOneSpatialOperator(func='ST_EQUALS'),
        'relate': HanaIsOneSpatialOperator(func='ST_RELATE'),
        'touches': HanaIsOneSpatialOperator(func='ST_TOUCHES'),
        'within': HanaIsValueSpatialOperator(func='ST_WITHINDISTANCE', op='<='),
    }

    def __init__(self, connection):
        super(DatabaseOperations, self).__init__(connection)

    def get_seq_name(self, table, column):
        return '%s_%s_seq' % (table, column)

    def autoinc_sql(self, table, column):
        seq_name = self.quote_name(self.get_seq_name(table, column))
        column = self.quote_name(column)
        table = self.quote_name(table)
        seq_sql = 'CREATE SEQUENCE %(seq_name)s RESET BY SELECT IFNULL(MAX(%(column)s),0) + 1 FROM %(table)s' % locals()
        return [seq_sql]

    def date_extract_sql(self, lookup_type, field_name):
        if lookup_type == 'week_day':
            # For consistency across backends, we return Sunday=1, Saturday=7.
            return 'MOD(WEEKDAY (%s) + 2,7)' % field_name
        else:
            return 'EXTRACT(%s FROM %s)' % (lookup_type, field_name)

    def date_trunc_sql(self, lookup_type, field_name):
        # very low tech, code should be optimized
        ltypes = {
            'year': 'YYYY',
            'month': 'YYYY-MM',
            'day': 'YYYY-MM-DD',
        }
        cur_type = ltypes.get(lookup_type)
        if not cur_type:
            return field_name
        sql = 'TO_DATE(TO_VARCHAR(%s, "%s"))' % (field_name, cur_type)
        return sql

    def no_limit_value(self):
        return None

    def quote_name(self, name):
        return '"%s"' % name.replace('"', '""').upper()

    def bulk_batch_size(self, fields, objs):
        return 2500

    def sql_flush(self, style, tables, sequences, allow_cascades=False):
        if tables:
            sql = [
                ' '.join([
                    style.SQL_KEYWORD('DELETE'),
                    style.SQL_KEYWORD('FROM'),
                    style.SQL_FIELD(self.quote_name(table)),
                ])
                for table in tables
            ]
            sql.extend(self.sequence_reset_by_name_sql(style, sequences))
            return sql
        else:
            return []

    def sequence_reset_by_name_sql(self, style, sequences):
        sql = []
        for sequence_info in sequences:
            table_name = sequence_info['table']
            column_name = sequence_info['column']
            seq_name = self.get_seq_name(table_name, column_name)
            sql.append(' '.join([
                'ALTER SEQUENCE',
                seq_name,
                'RESET BY SELECT IFNULL(MAX(',
                column_name,
                '),0) + 1 from',
                table_name,
            ]))
        return sql

    def sequence_reset_sql(self, style, model_list):
        from django.db import models
        output = []
        for model in model_list:
            for f in model._meta.local_fields:
                if isinstance(f, models.AutoField):
                    output.append(' '.join([
                        style.SQL_KEYWORD('ALTER SEQUENCE'),
                        style.SQL_TABLE(self.get_seq_name(model._meta.db_table, f.column)),
                        style.SQL_KEYWORD('RESET BY SELECT'),
                        style.SQL_FIELD('IFNULL(MAX('+f.column+'),0) + 1'),
                        style.SQL_KEYWORD('FROM'),
                        style.SQL_TABLE(model._meta.db_table),
                    ]))
                    break  # Only one AutoField is allowed per model, so don't bother continuing.
            for f in model._meta.many_to_many:
                if not f.rel.through:
                    output.append(' '.join([
                        style.SQL_KEYWORD('ALTER SEQUENCE'),
                        style.SQL_TABLE(self.get_seq_name(f.m2m_db_table(), 'id')),
                        style.SQL_KEYWORD('RESET BY SELECT'),
                        style.SQL_FIELD('IFNULL(MAX(id),0) + 1'),
                        style.SQL_KEYWORD('FROM'),
                        style.SQL_TABLE(f.m2m_db_table())
                    ]))
        return output

    def prep_for_iexact_query(self, x):
        return x

    def check_aggregate_support(self, aggregate):
        """
        Check that the backend supports the provided aggregate.

        This is used on specific backends to rule out known aggregates
        that are known to have faulty implementations. If the named
        aggregate function has a known problem, the backend should
        raise NotImplementedError.
        """
        if aggregate.sql_function in ('STDDEV_POP', 'VAR_POP'):
                raise NotImplementedError()

    def max_name_length(self):
        """
        Returns the maximum length of table and column names, or None if there
        is no limit.
        """
        return 127

    def start_transaction_sql(self):
        return ''

    def last_insert_id(self, cursor, table_name, pk_name):
        """
        Given a cursor object that has just performed an INSERT statement into
        a table that has an auto-incrementing ID, returns the newly created ID.

        This method also receives the table name and the name of the primary-key
        column.
        """
        seq_name = self.connection.ops.get_seq_name(table_name, pk_name)
        sql = 'select {}.currval from dummy'.format(seq_name)
        cursor.execute(sql)
        return cursor.fetchone()[0]

    def value_to_db_datetime(self, value):
        """
        Transform a datetime value to an object compatible with what is expected
        by the backend driver for datetime columns.
        """
        if value is None:
            return None
        if value.tzinfo:
            # HANA doesn't support timezone. If tzinfo is present truncate it.
            # Better set USE_TZ=False in settings.py
            import datetime
            return six.text_type(
                datetime.datetime(
                    value.year, value.month, value.day, value.hour, value.minute, value.second, value.microsecond
                )
            )
        return six.text_type(value)

    def lookup_cast(self, lookup_type, internal_type=None):
        if lookup_type in ('iexact', 'icontains', 'istartswith', 'iendswith'):
            return 'UPPER(%s)'
        return '%s'

    def convert_values(self, value, field):
        """
        Type conversion for boolean field. Keping values as 0/1 confuses
        the modelforms.
        """
        if (field and field.get_internal_type() in ('BooleanField', 'NullBooleanField') and value in (0, 1)):
            value = bool(value)
        return value

    # Decimal to Database. Django == 1.8
    def value_to_db_decimal(self, value, max_digits, decimal_places):
        return value or None

    # Decimal to Database. Django >= 1.9
    def adapt_decimalfield_value(self, value, max_digits=None, decimal_places=None):
        return value or None

    def modify_insert_params(self, placeholder, params):
        insert_param_groups = []
        for p in params:
            if isinstance(p, list):
                insert_param_groups.append([self.sanitize_bool(value) for value in p])
            else:
                # As of Django 1.9, modify_insert_params is also called in SQLInsertCompiler.field_as_sql.
                # When it's called from there, params is not a list inside a list, but only a list.
                insert_param_groups.append(self.sanitize_bool(p))
        return insert_param_groups

    def modify_update_params(self, params):
        return tuple(self.sanitize_bool(param) for param in params)

    def modify_params(self, params):
        return tuple(self.sanitize_geometry(param) for param in params)

    def sanitize_bool(self, param):
        if type(param) is bool:
            return 1 if param else 0
        return param

    def sanitize_geometry(self, param):
        if type(param) is WKTAdapter:
            return str(param)
        return param

    def get_db_converters(self, expression):
        converters = super(DatabaseOperations, self).get_db_converters(expression)
        internal_type = expression.output_field.get_internal_type()
        geometry_fields = (
            'PointField', 'LineStringField', 'PolygonField',
            'MultiPointField', 'MultiLineStringField', 'MultiPolygonField',
        )
        if internal_type == 'TextField':
            converters.append(self.convert_textfield_value)
        elif internal_type == 'BinaryField':
            converters.append(self.convert_binaryfield_value)
        elif internal_type in ['BooleanField', 'NullBooleanField']:
            converters.append(self.convert_booleanfield_value)
        elif internal_type == 'UUIDField':
            converters.append(self.convert_uuidfield_value)
        elif internal_type in geometry_fields:
            converters.append(self.convert_geometry_value)
        if hasattr(expression.output_field, 'geom_type'):
            converters.append(self.convert_geometry)
        return converters

    def convert_textfield_value(self, value, expression, connection, context):
        if isinstance(value, Database.NClob):
            value = force_text(value.read())
        return value

    def convert_binaryfield_value(self, value, expression, connection, context):
        if isinstance(value, Database.Blob):
            value = value.read()
        return value

    def convert_booleanfield_value(self, value, expression, connection, context):
        if value in (0, 1):
            value = bool(value)
        return value

    def convert_uuidfield_value(self, value, expression, connection, context):
        if value is not None:
            value = uuid.UUID(value)
        return value

    def convert_geometry_value(self, value, expression, connection, context):
        if value is not None:
            value = ''.join('{:02x}'.format(x) for x in value)
        return value

    def convert_geometry(self, value, expression, connection, context):
        if value:
            value = Geometry(value)
            if 'transformed_srid' in context:
                value.srid = context['transformed_srid']
        return value

    def _geo_db_type(self, f):
        return 'ST_%s' % f.geom_type

    def geo_db_type(self, f):
        internal_type = self._geo_db_type(f)
        return internal_type if f.geom_type == 'POINT' else 'ST_GEOMETRY'

    def get_distance(self, f, value, lookup_type):
        if not value:
            return []
        value = value[0]
        if isinstance(value, Distance):
            if f.geodetic(self.connection):
                raise ValueError('SAP HANA does not support distance queries on '
                                 'geometry fields with a geodetic coordinate system. '
                                 'Distance objects; use a numeric value of your '
                                 'distance in degrees instead.')
            else:
                dist_param = getattr(value, Distance.unit_attname(f.units_name(self.connection)))
        else:
            dist_param = value
        return [dist_param]

    def get_geom_placeholder(self, f, value, compiler):
        if value is None:
            placeholder = '%s'
        else:
            db_type = self._geo_db_type(f)
            placeholder = 'NEW %s(%%s, %s)' % (db_type, f.srid)

        if hasattr(value, 'as_sql'):
            sql, _ = compiler.compile(value)
            placeholder = placeholder % sql

        return placeholder

    def geometry_columns(self):
        from django_hana.models import HanaGeometryColumns
        return HanaGeometryColumns

    def spatial_ref_sys(self):
        from django_hana.models import HanaSpatialRefSys
        return HanaSpatialRefSys
