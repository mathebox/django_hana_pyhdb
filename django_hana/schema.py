
from django.db.backends.schema import BaseDatabaseSchemaEditor


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    # sql templates to configure (override)

    # sql_create_table = "CREATE TABLE %(table)s (%(definition)s)" # correct
    sql_create_table_unique = "UNIQUE (%(columns)s)" # don't know
    sql_rename_table = "RENAME TABLE %(old_table)s TO %(new_table)s" # changed
    sql_retablespace_table = "ALTER TABLE %(table)s MOVE TO %(new_tablespace)s"# maybe
    # sql_delete_table = "DROP TABLE %(table)s CASCADE" # correct

    sql_create_column = "ALTER TABLE %(table)s ADD (%(column)s %(definition)s)" # changed
    sql_alter_column = "ALTER TABLE %(table)s %(changes)s"
    sql_alter_column_type = "ALTER COLUMN %(column)s TYPE %(type)s"
    sql_alter_column_null = "ALTER COLUMN %(column)s DROP NOT NULL"
    sql_alter_column_not_null = "ALTER %(column)s NOT NULL" # changed
    sql_alter_column_default = "ALTER (%(column)s DEFAULT %(default)s)" # changed
    sql_alter_column_no_default = "ALTER (%(column)s DEFAULT NULL)" # changed
    sql_delete_column = "ALTER TABLE %(table)s DROP %(column)s" # changed
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
