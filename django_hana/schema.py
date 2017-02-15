from django.db.backends.base.schema import BaseDatabaseSchemaEditor

import django_hana


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    # sql templates to configure (override)

    sql_create_table_template = 'CREATE %(table_type)s TABLE %%(table)s (%%(definition)s)'
    sql_create_table = 'CREATE COLUMN TABLE %(table)s (%(definition)s)'
    sql_create_table_unique = 'UNIQUE (%(columns)s)'
    sql_rename_table = 'RENAME TABLE %(old_table)s TO %(new_table)s'
    sql_retablespace_table = 'ALTER TABLE %(table)s MOVE TO %(new_tablespace)s'
    # sql_delete_table = 'DROP TABLE %(table)s CASCADE'

    sql_create_column = 'ALTER TABLE %(table)s ADD (%(column)s %(definition)s)'
    sql_alter_column = 'ALTER TABLE %(table)s %(changes)s'
    sql_alter_column_type = 'ALTER (%(column)s %(type)s)'
    sql_alter_column_null = 'ALTER (%(column)s %(type)s NULL)'
    sql_alter_column_not_null = 'ALTER (%(column)s %(type)s NOT NULL)'
    sql_alter_column_default = 'ALTER (%(column)s %(definition)s DEFAULT %(default)s)'
    sql_alter_column_no_default = 'ALTER (%(column)s %(definition)s)'
    sql_delete_column = 'ALTER TABLE %(table)s DROP (%(column)s)'
    sql_rename_column = 'RENAME COLUMN %(table)s.%(old_column)s TO %(new_column)s'
    sql_update_with_default = 'UPDATE %(table)s SET %(column)s = %(default)s WHERE %(column)s IS NULL'

    sql_create_check = 'ALTER TABLE %(table)s ADD CONSTRAINT %(name)s CHECK (%(check)s)'
    sql_delete_check = 'ALTER TABLE %(table)s DROP CONSTRAINT %(name)s'

    sql_create_unique = 'ALTER TABLE %(table)s ADD CONSTRAINT %(name)s UNIQUE (%(columns)s)'
    sql_delete_unique = 'ALTER TABLE %(table)s DROP CONSTRAINT %(name)s'

    sql_create_fk = (
        'ALTER TABLE %(table)s ADD CONSTRAINT %(name)s FOREIGN KEY (%(column)s) '
        'REFERENCES %(to_table)s (%(to_column)s) ON DELETE CASCADE'
    )
    sql_create_inline_fk = None
    sql_delete_fk = 'ALTER TABLE %(table)s DROP CONSTRAINT %(name)s'

    sql_create_index = 'CREATE INDEX %(name)s ON %(table)s (%(columns)s)%(extra)s'
    sql_delete_index = 'DROP INDEX %(name)s'

    # sql_create_pk = 'ALTER TABLE %(table)s ADD CONSTRAINT %(name)s PRIMARY KEY (%(columns)s)'
    # sql_delete_pk = 'ALTER TABLE %(table)s DROP CONSTRAINT %(name)s'

    def skip_default(self, field):
        # When altering a column, SAP HANA requires the column definition. This is not the case for other databases.
        # So Django does not pass the column definition to the sql format strings. Since Django does not use database
        # default. (It creates a column with default values and drop the contraint immediately.) In order to avoid
        # entire methods of Django to support this behavior, we will skip creating default constraints entirely.
        return True

    def create_model(self, model):
        # To support creating column and row table, we have to use this workaround. It sets the sql format string
        # according to the table type of the model.
        store_type = self.connection.settings_dict.get('DEFAULT_MODEL_STORE', 'COLUMN')
        table_type = django_hana.MODEL_STORE.get(model.__name__, store_type)
        self.sql_create_table = self.sql_create_table_template % {'table_type': table_type}
        super(DatabaseSchemaEditor, self).create_model(model)
