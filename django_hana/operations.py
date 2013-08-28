from __future__ import unicode_literals

from django.db.backends import BaseDatabaseOperations
from django.core.management.color import color_style

class DatabaseOperations(BaseDatabaseOperations):
    compiler_module = "django_hana.compiler"

    def __init__(self, connection):
        super(DatabaseOperations, self).__init__(connection)

    def get_seq_name(self,table,column):
        return self.connection.default_schema+"_"+table+"_"+column+"_seq"

    def autoinc_sql(self, table, column):
        seq_name=self.get_seq_name(table,column)
        seq_sql="""
CREATE SEQUENCE %(seq_name)s RESET BY SELECT IFNULL(MAX(%(column)s),0) + 1 FROM %(table)s
""" % locals()
        return [seq_sql]

    def date_extract_sql(self, lookup_type, field_name):
        if lookup_type == 'week_day':
            # For consistency across backends, we return Sunday=1, Saturday=7.
            return "MOD(WEEKDAY (%s) + 2,7)" % field_name
        else:
            return "EXTRACT(%s FROM %s)" % (lookup_type, field_name)

    def date_trunc_sql(self, lookup_type, field_name):
        # very low tech, code should be optimized
        ltypes = {'year':'YYYY','month':'YYYY-MM','day':'YYYY-MM-DD'}
        cur_type = ltypes.get(lookup_type)
        if not cur_type:
            return field_name
        sql = "TO_DATE(TO_VARCHAR(%s, '%s'))" % (field_name, cur_type)
        return sql

    def no_limit_value(self):
        return None

    def quote_name(self, name):
        #don't quote
        return name


    def sql_flush(self, style, tables, sequences):
        if tables:
            sql = ['%s %s %s;' % (style.SQL_KEYWORD('TRUNCATE'),style.SQL_KEYWORD('TABLE'),style.SQL_FIELD(self.quote_name(table))) for table in tables]
            sql.extend(self.sequence_reset_by_name_sql(style, sequences))
            return sql
        else:
            return []

    def sequence_reset_by_name_sql(self, style, sequences):
        sql = []
        for sequence_info in sequences:
            table_name = sequence_info['table']
            column_name = sequence_info['column']
            seq_name=self.get_seq_name(table_name,column_name)
            sql.append("ALTER SEQUENCE "+seq_name+" RESET BY SELECT IFNULL(MAX("+column_name+"),0) + 1 from "+table_name + ';')
        return sql

    def sequence_reset_sql(self, style, model_list):
        from django.db import models
        output = []
        qn = self.quote_name
        for model in model_list:
            for f in model._meta.local_fields:
                if isinstance(f, models.AutoField):
                    output.append("%s %s %s %s %s %s" % \
                        (style.SQL_KEYWORD("ALTER SEQUENCE"),
                        style.SQL_TABLE(self.get_seq_name(model._meta.db_table,f.column)),
                        style.SQL_KEYWORD("RESET BY SELECT"),
                        style.SQL_FIELD("IFNULL(MAX("+f.column+"),0) + 1"),
                        style.SQL_KEYWORD("FROM"),
                        style.SQL_TABLE(model._meta.db_table)))
                    break # Only one AutoField is allowed per model, so don't bother continuing.
            for f in model._meta.many_to_many:
                if not f.rel.through:
                    output.append("%s %s %s %s %s %s" % \
                        (style.SQL_KEYWORD("ALTER SEQUENCE"),
                        style.SQL_TABLE(self.get_seq_name(f.m2m_db_table(),"id")),
                        style.SQL_KEYWORD("RESET BY SELECT"),
                        style.SQL_FIELD("IFNULL(MAX(id),0) + 1"),
                        style.SQL_KEYWORD("FROM"),
                        style.SQL_TABLE(f.m2m_db_table())))
        return output

    def prep_for_iexact_query(self, x):
        return x

    def check_aggregate_support(self, aggregate):
        """Check that the backend supports the provided aggregate

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
            is no limit."""
        return 127


    def start_transaction_sql(self):
        return ""

    def last_insert_id(self, cursor, table_name, pk_name):
        """
        Given a cursor object that has just performed an INSERT statement into
        a table that has an auto-incrementing ID, returns the newly created ID.

        This method also receives the table name and the name of the primary-key
        column.
        """
        cursor.execute('select '+self.connection.ops.get_seq_name(table_name,pk_name)+'.currval from dummy')
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
            return unicode(datetime.datetime(value.year,value.month,value.day,value.hour,\
                    value.minute,value.second,value.microsecond))
        return unicode(value)

    def lookup_cast(self, lookup_type):
        if lookup_type in ('iexact', 'icontains', 'istartswith', 'iendswith'):
            return "UPPER(%s)"
        return "%s"
