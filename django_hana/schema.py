
from django.db.backends.base.schema import BaseDatabaseSchemaEditor, _related_non_m2m_objects
from django.utils import six

import django_hana


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    # sql templates to configure (override)

    sql_create_table = 'CREATE %(table_type)s TABLE %(table)s (%(definition)s)'
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
        # foreign key columns should not have a default value
        return field.column.endswith('_id')

    def prepare_default(self, value):
        if isinstance(value, six.string_types):
            return '"%s"' % (value,)
        return str(value)

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
                definition += ' CHECK (%s)' % db_params['check']
            # Autoincrement SQL (for backends with inline variant)
            col_type_suffix = field.db_type_suffix(connection=self.connection)
            if col_type_suffix:
                definition += ' %s' % col_type_suffix
            params.extend(extra_params)
            # FK
            if field.rel and field.db_constraint:
                to_table = field.rel.to._meta.db_table
                to_column = field.rel.to._meta.get_field(field.rel.field_name).column
                if self.connection.features.supports_foreign_keys:
                    self.deferred_sql.append(self._create_fk_sql(model, field, '_fk_%(to_table)s_%(to_column)s'))
                elif self.sql_create_inline_fk:
                    definition += ' ' + self.sql_create_inline_fk % {
                        'to_table': self.quote_name(to_table),
                        'to_column': self.quote_name(to_column),
                    }
            # Add the SQL to our big list
            column_sqls.append('%s %s' % (
                self.quote_name(field.column),
                definition,
            ))
            # Autoincrement SQL (for backends with post table definition variant)
            if field.get_internal_type() == 'AutoField':
                autoinc_sql = self.connection.ops.autoinc_sql(model._meta.db_table, field.column)
                if autoinc_sql:
                    self.deferred_sql.extend(autoinc_sql)

        # Add any unique_togethers
        for fields in model._meta.unique_together:
            columns = [model._meta.get_field(field).column for field in fields]
            column_sqls.append(self.sql_create_table_unique % {
                'columns': ', '.join(self.quote_name(column) for column in columns),
            })
        # Make the table
        store_type = self.connection.settings_dict.get('DEFAULT_MODEL_STORE', 'COLUMN')
        table_type = django_hana.MODEL_STORE.get(model.__name__, store_type)
        sql = self.sql_create_table % {
            'table_type': table_type,
            'table': self.quote_name(model._meta.db_table),
            'definition': ', '.join(column_sqls)
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

    def add_field(self, model, field):
        """
        Creates a field on a model.
        Usually involves adding a column, but may involve adding a
        table instead (for M2M fields)
        """
        # Special-case implicit M2M tables
        if field.many_to_many and field.rel.through._meta.auto_created:
            return self.create_model(field.rel.through)
        # Get the column's definition
        definition, params = self.column_sql(model, field, include_default=True)
        # It might not actually have a column behind it
        if definition is None:
            return
        # Check constraints can go on the column SQL here
        db_params = field.db_parameters(connection=self.connection)
        if db_params['check']:
            definition += ' CHECK (%s)' % db_params['check']
        # Build the SQL and run it
        sql = self.sql_create_column % {
            'table': self.quote_name(model._meta.db_table),
            'column': self.quote_name(field.column),
            'definition': definition,
        }
        self.execute(sql, params)
        # Drop the default if we need to
        # (Django usually does not use in-database defaults)
        if not self.skip_default(field) and field.default is not None:
            sql = self.sql_alter_column % {
                'table': self.quote_name(model._meta.db_table),
                'changes': self.sql_alter_column_no_default % {
                    'column': self.quote_name(field.column),
                    'definition': definition,
                }
            }
            self.execute(sql)
        # Add an index, if required
        if field.db_index and not field.unique:
            self.deferred_sql.append(self._create_index_sql(model, [field]))
        # Add any FK constraints later
        if field.rel and self.connection.features.supports_foreign_keys and field.db_constraint:
            self.deferred_sql.append(self._create_fk_sql(model, field, '_fk_%(to_table)s_%(to_column)s'))
        # Reset connection if required
        if self.connection.features.connection_persists_old_columns:
            self.connection.close()

    def remove_field(self, model, field):
        """
        Removes a field from a model. Usually involves deleting a column,
        but for M2Ms may involve deleting a table.
        """
        # Special-case implicit M2M tables
        if field.many_to_many and field.rel.through._meta.auto_created:
            return self.delete_model(field.rel.through)
        # It might not actually have a column behind it
        if field.db_parameters(connection=self.connection)['type'] is None:
            return
        # Drop any FK constraints, MySQL requires explicit deletion
        if field.rel:
            # second param in original implementation: [field.column]
            # maybe HANA does not automatically removes all contraints of
            # a column when removing this column
            fk_names = self._constraint_names(model, None, foreign_key=True)
            for fk_name in fk_names:
                self.execute(self._delete_constraint_sql(self.sql_delete_fk, model, fk_name))
        # Delete the column
        sql = self.sql_delete_column % {
            'table': self.quote_name(model._meta.db_table),
            'column': self.quote_name(field.column),
        }
        self.execute(sql)
        # Reset connection if required
        if self.connection.features.connection_persists_old_columns:
            self.connection.close()

    def _alter_field(self, model, old_field, new_field, old_type, new_type,
                     old_db_params, new_db_params, strict=False):
        """Actually perform a "physical" (non-ManyToMany) field update."""

        # Drop any FK constraints, we'll remake them later
        fks_dropped = set()
        if old_field.rel and old_field.db_constraint:
            fk_names = self._constraint_names(model, [old_field.column], foreign_key=True)
            if strict and len(fk_names) != 1:
                raise ValueError('Found wrong number (%s) of foreign key constraints for %s.%s' % (
                    len(fk_names),
                    model._meta.db_table,
                    old_field.column,
                ))
            for fk_name in fk_names:
                fks_dropped.add((old_field.column,))
                self.execute(self._delete_constraint_sql(self.sql_delete_fk, model, fk_name))
        # Has unique been removed?
        if old_field.unique and (not new_field.unique or (not old_field.primary_key and new_field.primary_key)):
            # Find the unique constraint for this field
            constraint_names = self._constraint_names(model, [old_field.column], unique=True)
            if strict and len(constraint_names) != 1:
                raise ValueError('Found wrong number (%s) of unique constraints for %s.%s' % (
                    len(constraint_names),
                    model._meta.db_table,
                    old_field.column,
                ))
            for constraint_name in constraint_names:
                self.execute(self._delete_constraint_sql(self.sql_delete_unique, model, constraint_name))
        # Drop incoming FK constraints if we're a primary key and things are going
        # to change.
        if old_field.primary_key and new_field.primary_key and old_type != new_type:
            # '_meta.related_field' also contains M2M reverse fields, these
            # will be filtered out
            for _old_rel, new_rel in _related_non_m2m_objects(old_field, new_field):
                rel_fk_names = self._constraint_names(
                    new_rel.related_model, [new_rel.field.column], foreign_key=True
                )
                for fk_name in rel_fk_names:
                    self.execute(self._delete_constraint_sql(self.sql_delete_fk, new_rel.related_model, fk_name))
        # Removed an index? (no strict check, as multiple indexes are possible)
        if (old_field.db_index and not new_field.db_index and
                not old_field.unique and not
                (not new_field.unique and old_field.unique)):
            # Find the index for this field
            index_names = self._constraint_names(model, [old_field.column], index=True)
            for index_name in index_names:
                self.execute(self._delete_constraint_sql(self.sql_delete_index, model, index_name))
        # Change check constraints?
        if old_db_params['check'] != new_db_params['check'] and old_db_params['check']:
            constraint_names = self._constraint_names(model, [old_field.column], check=True)
            if strict and len(constraint_names) != 1:
                raise ValueError('Found wrong number (%s) of check constraints for %s.%s' % (
                    len(constraint_names),
                    model._meta.db_table,
                    old_field.column,
                ))
            for constraint_name in constraint_names:
                self.execute(self._delete_constraint_sql(self.sql_delete_check, model, constraint_name))
        # Have they renamed the column?
        if old_field.column != new_field.column:
            self.execute(self.sql_rename_column % {
                'table': self.quote_name(model._meta.db_table),
                'old_column': self.quote_name(old_field.column),
                'new_column': self.quote_name(new_field.column),
                'type': new_type,
            })
        # Next, start accumulating actions to do
        actions = []
        null_actions = []
        post_actions = []
        # Type change?
        if old_type != new_type:
            fragment, other_actions = self._alter_column_type_sql(
                model._meta.db_table, old_field, new_field, new_type
            )
            actions.append(fragment)
            post_actions.extend(other_actions)
        # When changing a column NULL constraint to NOT NULL with a given
        # default value, we need to perform 4 steps:
        #  1. Add a default for new incoming writes
        #  2. Update existing NULL rows with new default
        #  3. Replace NULL constraint with NOT NULL
        #  4. Drop the default again.
        # Default change?
        old_default = self.effective_default(old_field)
        new_default = self.effective_default(new_field)
        needs_database_default = (
            old_default != new_default and
            new_default is not None and
            not self.skip_default(new_field)
        )
        if needs_database_default:
            definition, params = self.column_sql(model, new_field, include_default=False)
            if self.connection.features.requires_literal_defaults:
                # Some databases can't take defaults as a parameter (oracle)
                # If this is the case, the individual schema backend should
                # implement prepare_default
                actions.append((
                    self.sql_alter_column_default % {
                        'column': self.quote_name(new_field.column),
                        'definition': definition,
                        'default': self.prepare_default(new_default),
                    },
                    [],
                ))
            else:
                actions.append((
                    self.sql_alter_column_default % {
                        'column': self.quote_name(new_field.column),
                        'definition': definition,
                        'default': '%s',
                    },
                    [new_default],
                ))
        # Nullability change?
        if old_field.null != new_field.null:
            if (self.connection.features.interprets_empty_strings_as_nulls and
                    new_field.get_internal_type() in ('CharField', 'TextField')):
                # The field is nullable in the database anyway, leave it alone
                pass
            elif new_field.null:
                null_actions.append((
                    self.sql_alter_column_null % {
                        'column': self.quote_name(new_field.column),
                        'type': new_type,
                    },
                    [],
                ))
            else:
                null_actions.append((
                    self.sql_alter_column_not_null % {
                        'column': self.quote_name(new_field.column),
                        'type': new_type,
                    },
                    [],
                ))
        # Only if we have a default and there is a change from NULL to NOT NULL
        four_way_default_alteration = (
            new_field.has_default() and
            (old_field.null and not new_field.null)
        )
        if actions or null_actions:
            if not four_way_default_alteration:
                # If we don't have to do a 4-way default alteration we can
                # directly run a (NOT) NULL alteration
                actions = actions + null_actions
            # Combine actions together if we can (e.g. postgres)
            if self.connection.features.supports_combined_alters and actions:
                sql, params = tuple(zip(*actions))
                actions = [(', '.join(sql), sum(params, []))]
            # Apply those actions
            for sql, params in actions:
                self.execute(
                    self.sql_alter_column % {
                        'table': self.quote_name(model._meta.db_table),
                        'changes': sql,
                    },
                    params,
                )
            if four_way_default_alteration:
                # Update existing rows with default value
                self.execute(
                    self.sql_update_with_default % {
                        'table': self.quote_name(model._meta.db_table),
                        'column': self.quote_name(new_field.column),
                        'default': '%s',
                    },
                    [new_default],
                )
                # Since we didn't run a NOT NULL change before we need to do it
                # now
                for sql, params in null_actions:
                    self.execute(
                        self.sql_alter_column % {
                            'table': self.quote_name(model._meta.db_table),
                            'changes': sql,
                        },
                        params,
                    )
        if post_actions:
            for sql, params in post_actions:
                self.execute(sql, params)
        # Added a unique?
        if (not old_field.unique and new_field.unique) or (
            old_field.primary_key and not new_field.primary_key and new_field.unique
        ):
            self.execute(self._create_unique_sql(model, [new_field.column]))
        # Added an index?
        if (not old_field.db_index and new_field.db_index and
                not new_field.unique and not
                (not old_field.unique and new_field.unique)):
            self.execute(self._create_index_sql(model, [new_field], suffix='_uniq'))
        # Type alteration on primary key? Then we need to alter the column
        # referring to us.
        rels_to_update = []
        if old_field.primary_key and new_field.primary_key and old_type != new_type:
            rels_to_update.extend(_related_non_m2m_objects(old_field, new_field))
        # Changed to become primary key?
        # Note that we don't detect unsetting of a PK, as we assume another field
        # will always come along and replace it.
        if not old_field.primary_key and new_field.primary_key:
            # First, drop the old PK
            constraint_names = self._constraint_names(model, primary_key=True)
            if strict and len(constraint_names) != 1:
                raise ValueError('Found wrong number (%s) of PK constraints for %s' % (
                    len(constraint_names),
                    model._meta.db_table,
                ))
            for constraint_name in constraint_names:
                self.execute(self._delete_constraint_sql(self.sql_delete_pk, model, constraint_name))
            # Make the new one
            self.execute(
                self.sql_create_pk % {
                    'table': self.quote_name(model._meta.db_table),
                    'name': self.quote_name(self._create_index_name(model, [new_field.column], suffix='_pk')),
                    'columns': self.quote_name(new_field.column),
                }
            )
            # Update all referencing columns
            rels_to_update.extend(_related_non_m2m_objects(old_field, new_field))
        # Handle our type alters on the other end of rels from the PK stuff above
        for old_rel, new_rel in rels_to_update:
            rel_db_params = new_rel.field.db_parameters(connection=self.connection)
            rel_type = rel_db_params['type']
            fragment, other_actions = self._alter_column_type_sql(
                new_rel.related_model._meta.db_table, old_rel.field, new_rel.field, rel_type
            )
            self.execute(
                self.sql_alter_column % {
                    'table': self.quote_name(new_rel.related_model._meta.db_table),
                    'changes': fragment[0],
                },
                fragment[1],
            )
            for sql, params in other_actions:
                self.execute(sql, params)
        # Does it have a foreign key?
        if (new_field.rel and
                (fks_dropped or not old_field.rel or not old_field.db_constraint) and
                new_field.db_constraint):
            self.execute(self._create_fk_sql(model, new_field, '_fk_%(to_table)s_%(to_column)s'))
        # Rebuild FKs that pointed to us if we previously had to drop them
        if old_field.primary_key and new_field.primary_key and old_type != new_type:
            for rel in new_field.model._meta.related_objects:
                if not rel.many_to_many:
                    self.execute(self._create_fk_sql(rel.related_model, rel.field, '_fk'))
        # Does it have check constraints we need to add?
        if old_db_params['check'] != new_db_params['check'] and new_db_params['check']:
            self.execute(
                self.sql_create_check % {
                    'table': self.quote_name(model._meta.db_table),
                    'name': self.quote_name(self._create_index_name(model, [new_field.column], suffix='_check')),
                    'column': self.quote_name(new_field.column),
                    'check': new_db_params['check'],
                }
            )
        # Drop the default if we need to
        # (Django usually does not use in-database defaults)
        if needs_database_default:
            definition, params = self.column_sql(model, new_field, include_default=False)
            sql = self.sql_alter_column % {
                'table': self.quote_name(model._meta.db_table),
                'changes': self.sql_alter_column_no_default % {
                    'column': self.quote_name(new_field.column),
                    'definition': definition,
                }
            }
            self.execute(sql)
        # Reset connection if required
        if self.connection.features.connection_persists_old_columns:
            self.connection.close()

    def _alter_column_type_sql(self, table, old_field, new_field, new_type):
        """
        Hook to specialize column type alteration for different backends,
        for cases when a creation type is different to an alteration type
        (e.g. SERIAL in PostgreSQL, PostGIS fields).

        Should return two things; an SQL fragment of (sql, params) to insert
        into an ALTER TABLE statement, and a list of extra (sql, params) tuples
        to run once the field is altered.
        """
        return (
            (
                self.sql_alter_column_type % {
                    'column': self.quote_name(new_field.column),
                    'type': new_type,
                },
                [],
            ),
            [],
        )
