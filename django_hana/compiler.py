from itertools import izip

from django.core.exceptions import FieldError
from django.db import transaction
from django.db.backends.util import truncate_name
from django.db.models.query_utils import select_related_descend
from django.db.models.sql.constants import *
from django.db.models.sql.datastructures import EmptyResultSet
from django.db.models.sql.expressions import SQLEvaluator
from django.db.models.sql.query import get_order_dir, Query
from django.db.utils import DatabaseError
from django.db import models
from django.db.models.sql import compiler

class SQLCompiler(compiler.SQLCompiler):
    def resolve_columns(self, row, fields=()):
        """
        Taken from fox:
        https://github.com/django/django/commit/9f6859e1ea

        Basically a hook, where we call convert_values() which would turn 0/1 to Booleans.
        """
        values = []
        index_extra_select = len(self.query.extra_select.keys())
        for value, field in map(None, row[index_extra_select:], fields):
            values.append(self.query.convert_values(value, field, connection=self.connection))
        return row[:index_extra_select] + tuple(values)

class SQLInsertCompiler(compiler.SQLInsertCompiler, SQLCompiler):
    def as_sql(self):
        qn = self.connection.ops.quote_name
        opts = self.query.model._meta
        result = ['INSERT INTO %s' % qn(opts.db_table)]

        has_fields = bool(self.query.fields)
        fields = self.query.fields if has_fields else [opts.pk]

        pkinfields=False #when explicit pk value is provided
        if opts.pk in fields:
            pkinfields=True;

        if opts.has_auto_field and not pkinfields:
            # get auto field name
            auto_field_column=opts.auto_field.db_column or opts.auto_field.column
            result.append('('+auto_field_column+',%s)' % ', '.join([qn(f.column) for f in fields]))
        else:
            result.append('(%s)' % ', '.join([qn(f.column) for f in fields]))

        if has_fields:
            params=[]
            for obj in self.query.objs:
                vals=[]
                for f in fields:
                    val=f.get_db_prep_save(getattr(obj, f.attname) if self.query.raw else f.pre_save(obj, True), connection=self.connection)
                    vals.append(val)
                params.append(vals)

            values=params
        else:
            values = [[self.connection.ops.pk_default_value()] for obj in self.query.objs]
            params = [[]]
            fields = [None]

        placeholders=[]
        for val in values:
            p=[]
            for field,v in izip(fields,val):
                p.append(self.placeholder(field,v))
            placeholders.append(p)


        seq_func=''
        # don't insert call to seq function if explicit pk field value is provided
        if opts.has_auto_field and not pkinfields:
            auto_field_column=opts.auto_field.db_column or opts.auto_field.column
            seq_func=self.connection.ops.get_seq_name(opts.db_table,auto_field_column)+'.nextval, '


        return [
            (" ".join(result + ["VALUES ("+seq_func+"%s)" % ", ".join(p)]), vals)
            for p, vals in izip(placeholders, params)
        ]


class SQLDeleteCompiler(compiler.SQLDeleteCompiler,SQLCompiler):
    pass

class SQLUpdateCompiler(compiler.SQLUpdateCompiler,SQLCompiler):
    pass

class SQLAggregateCompiler(compiler.SQLAggregateCompiler, SQLCompiler):
    pass

class SQLDateCompiler(compiler.SQLDateCompiler,SQLCompiler):
    pass
