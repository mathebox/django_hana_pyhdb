from django.db.models.sql import compiler

from django_hana import compat


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

    def as_sql(self, *args, **kwargs):
        result, params = super(SQLCompiler, self).as_sql(*args, **kwargs)
        update_params = self.connection.ops.modify_params(params)
        return result, update_params


class SQLInsertCompiler(compiler.SQLInsertCompiler, SQLCompiler):
    def as_sql(self):
        qn = self.connection.ops.quote_name
        opts = self.query.model._meta
        result = ['INSERT INTO %s' % qn(opts.db_table)]

        has_fields = bool(self.query.fields)
        fields = self.query.fields if has_fields else [opts.pk]

        pkinfields = False  # when explicit pk value is provided
        if opts.pk in fields:
            pkinfields = True

        if opts.has_auto_field and not pkinfields:
            # get auto field name
            auto_field_column = opts.auto_field.db_column or opts.auto_field.column
            result.append('('+auto_field_column+',%s)' % ', '.join([qn(f.column) for f in fields]))
        else:
            result.append('(%s)' % ', '.join([qn(f.column) for f in fields]))

        if has_fields:
            params = values = [
                [
                    f.get_db_prep_save(
                        getattr(obj, f.attname) if self.query.raw else f.pre_save(obj, True),
                        connection=self.connection
                    ) for f in fields
                ]
                for obj in self.query.objs
            ]
        else:
            values = [[self.connection.ops.pk_default_value()] for obj in self.query.objs]
            params = [[]]
            fields = [None]

        placeholders = [
            [compat.createPlaceholder(self, field, v) for field, v in zip(fields, val)]
            for val in values
        ]

        seq_func = ''
        # don't insert call to seq function if explicit pk field value is provided
        if opts.has_auto_field and not pkinfields:
            auto_field_column = opts.auto_field.db_column or opts.auto_field.column
            seq_func = self.connection.ops.get_seq_name(opts.db_table, auto_field_column) + '.nextval, '

        params = self.connection.ops.modify_insert_params(placeholders, params)
        params = self.connection.ops.modify_params(params)

        can_bulk = (
            not any(hasattr(field, 'get_placeholder') for field in fields)
            and self.connection.features.has_bulk_insert
        )

        if can_bulk and len(params) > 1:
            placeholders = ['%s'] * len(fields)
            return [
                (' '.join(result + ['VALUES (' + seq_func + '%s)' % ', '.join(placeholders)]), params)
            ]

        return [
            (' '.join(result + ['VALUES (' + seq_func + '%s)' % ', '.join(p)]), vals)
            for p, vals in zip(placeholders, params)
        ]

    def execute_sql(self, return_id=False):
        assert not (return_id and len(self.query.objs) != 1)
        self.return_id = return_id
        with self.connection.cursor() as cursor:
            for sql, params in self.as_sql():
                if isinstance(params, (list, tuple)) and isinstance(params[0], (list, tuple)):
                    cursor.executemany(sql, params)
                else:
                    cursor.execute(sql, params)
            if not (return_id and cursor):
                return
            if self.connection.features.can_return_id_from_insert:
                return self.connection.ops.fetch_returned_insert_id(cursor)
            return self.connection.ops.last_insert_id(
                cursor,
                self.query.get_meta().db_table,
                self.query.get_meta().pk.column,
            )


class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SQLCompiler):
    pass


class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SQLCompiler):
    def as_sql(self, *args, **kwargs):
        result, params = super(SQLUpdateCompiler, self).as_sql(*args, **kwargs)
        update_params = self.connection.ops.modify_update_params(params)
        return result, update_params


class SQLAggregateCompiler(compiler.SQLAggregateCompiler, SQLCompiler):
    pass
