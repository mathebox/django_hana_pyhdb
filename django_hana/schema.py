
from django.db.backends.base.schema import BaseDatabaseSchemaEditor

import django_hana


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    # sql templates to configure (override)

    sql_create_table = "CREATE %(table_type)s TABLE %(table)s (%(definition)s)" # correct
    sql_create_table_unique = "UNIQUE (%(columns)s)" # don't know
    sql_rename_table = "RENAME TABLE %(old_table)s TO %(new_table)s" # changed
    sql_retablespace_table = "ALTER TABLE %(table)s MOVE TO %(new_tablespace)s"# maybe
    # sql_delete_table = "DROP TABLE %(table)s CASCADE" # correct

    sql_create_column = "ALTER TABLE %(table)s ADD (%(column)s %(definition)s)" # changed
    sql_alter_column = "ALTER TABLE %(table)s %(changes)s"
    sql_alter_column_type = "ALTER (%(column)s %(type)s)" # changed
    sql_alter_column_null = "ALTER (%(column)s %(type)s)" # changed
    sql_alter_column_not_null = "ALTER (%(column)s %(type)s NOT NULL)" # changed
    sql_alter_column_default = "ALTER (%(column)s DEFAULT %(default)s)" # changed
    sql_alter_column_no_default = "ALTER (%(column)s DEFAULT NULL)" # changed
    sql_delete_column = "ALTER TABLE %(table)s DROP (%(column)s)" # changed
    sql_rename_column = "ALTER TABLE %(table)s RENAME COLUMN %(old_column)s TO %(new_column)s"
    sql_update_with_default = "UPDATE %(table)s SET %(column)s = %(default)s WHERE %(column)s IS NULL"

    sql_create_check = "ALTER TABLE %(table)s ADD CONSTRAINT %(name)s CHECK (%(check)s)"
    sql_delete_check = "ALTER TABLE %(table)s DROP CONSTRAINT %(name)s"

    sql_create_unique = "ALTER TABLE %(table)s ADD CONSTRAINT %(name)s UNIQUE (%(columns)s)"
    sql_delete_unique = "ALTER TABLE %(table)s DROP CONSTRAINT %(name)s"

    sql_create_fk = "ALTER TABLE %(table)s ADD CONSTRAINT %(name)s FOREIGN KEY (%(column)s) REFERENCES %(to_table)s (%(to_column)s) ON DELETE CASCADE"
    sql_create_inline_fk = None
    sql_delete_fk = "ALTER TABLE %(table)s DROP CONSTRAINT %(name)s"

    sql_create_index = "CREATE INDEX %(name)s ON %(table)s (%(columns)s)%(extra)s"
    sql_delete_index = "DROP INDEX %(name)s"

    # sql_create_pk = "ALTER TABLE %(table)s ADD CONSTRAINT %(name)s PRIMARY KEY (%(columns)s)" # correct
    # sql_delete_pk = "ALTER TABLE %(table)s DROP CONSTRAINT %(name)s" # correct

    def skip_default(self, field):
        # foreign key columns should not have a default value
        return field.column.endswith("_id")

    def create_model(self, model):
        """
        Takes a model and creates a table for it in the database.
        Will also create any accompanying indexes or unique constraints.
        """
        # Create column SQL, add FK deferreds if needed
        column_sqls = []
        params = []
        for field in model._meta.local_fields:
            # SQL
            definition, extra_params = self.column_sql(model, field)
            if definition is None:
                continue
            # Check constraints can go on the column SQL here
            db_params = field.db_parameters(connection=self.connection)
            if db_params['check']:
                definition += " CHECK (%s)" % db_params['check']
            # Autoincrement SQL (for backends with inline variant)
            col_type_suffix = field.db_type_suffix(connection=self.connection)
            if col_type_suffix:
                definition += " %s" % col_type_suffix
            params.extend(extra_params)
            # FK
            if field.rel and field.db_constraint:
                to_table = field.rel.to._meta.db_table
                to_column = field.rel.to._meta.get_field(field.rel.field_name).column
                if self.connection.features.supports_foreign_keys:
                    self.deferred_sql.append(self._create_fk_sql(model, field, "_fk_%(to_table)s_%(to_column)s"))
                elif self.sql_create_inline_fk:
                    definition += " " + self.sql_create_inline_fk % {
                        "to_table": self.quote_name(to_table),
                        "to_column": self.quote_name(to_column),
                    }
            # Add the SQL to our big list
            column_sqls.append("%s %s" % (
                self.quote_name(field.column),
                definition,
            ))
            # Autoincrement SQL (for backends with post table definition variant)
            if field.get_internal_type() == "AutoField":
                autoinc_sql = self.connection.ops.autoinc_sql(model._meta.db_table, field.column)
                if autoinc_sql:
                    self.deferred_sql.extend(autoinc_sql)

        # Add any unique_togethers
        for fields in model._meta.unique_together:
            columns = [model._meta.get_field(field).column for field in fields]
            column_sqls.append(self.sql_create_table_unique % {
                "columns": ", ".join(self.quote_name(column) for column in columns),
            })
        # Make the table
        table_type = django_hana.MODEL_STORE.get(model.__name__, self.connection.settings_dict.get('DEFAULT_MODEL_STORE', 'COLUMN'))
        sql = self.sql_create_table % {
            "table_type": table_type,
            "table": self.quote_name(model._meta.db_table),
            "definition": ", ".join(column_sqls)
        }
        if model._meta.db_tablespace:
            tablespace_sql = self.connection.ops.tablespace_sql(model._meta.db_tablespace)
            if tablespace_sql:
                sql += ' ' + tablespace_sql
        # Prevent using [] as params, in the case a literal '%' is used in the definition
        self.execute(sql, params or None)

        # Add any field index and index_together's (deferred as SQLite3 _remake_table needs it)
        self.deferred_sql.extend(self._model_indexes_sql(model))

        # Make M2M tables
        for field in model._meta.local_many_to_many:
            if field.rel.through._meta.auto_created:
                self.create_model(field.rel.through)
