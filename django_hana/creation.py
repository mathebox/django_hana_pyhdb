from django.db.backends.creation import BaseDatabaseCreation
from django.db.backends.util import truncate_name
import django_hana

class DatabaseCreation(BaseDatabaseCreation):
    data_types = {
        'AutoField':         'int',
        'BooleanField':      'tinyint',
        'CharField':         'nvarchar(%(max_length)s)',
        'CommaSeparatedIntegerField': 'nvarchar(%(max_length)s)',
        'DateField':         'date',
        'DateTimeField':     'timestamp',
        'DecimalField':      'decimal(%(max_digits)s, %(decimal_places)s)',
        'FileField':         'nvarchar(%(max_length)s)',
        'FilePathField':     'nvarchar(%(max_length)s)',
        'FloatField':        'float',
        'IntegerField':      'int',
        'BigIntegerField':   'bigint',
        'IPAddressField':    'nvarchar(15)',
        'GenericIPAddressField': 'nvarchar(39)',
        'NullBooleanField':  'int',
        'OneToOneField':     'int',
        'PositiveIntegerField': 'int',
        'PositiveSmallIntegerField': 'smallint',
        'SlugField':         'nvarchar(%(max_length)s)',
        'SmallIntegerField': 'smallint',
        'TextField':         'nclob',
        'TimeField':         'time',
    }
    
    def sql_create_model(self, model, style, known_models=set()):
        """
        Returns the SQL required to create a single model, as a tuple of:
            (list_of_sql, pending_references_dict)
        """
        opts = model._meta
        if not opts.managed or opts.proxy:
            return [], {}
        final_output = []
        table_output = []
        pending_references = {}
        qn = self.connection.ops.quote_name
        for f in opts.local_fields:
            col_type = f.db_type(connection=self.connection)
            tablespace = f.db_tablespace or opts.db_tablespace
            if col_type is None:
                # Skip ManyToManyFields, because they're not represented as
                # database columns in this table.
                continue
            # Make the definition (e.g. 'foo VARCHAR(30)') for this field.
            field_output = [style.SQL_FIELD(qn(f.column)),
                style.SQL_COLTYPE(col_type)]
            if not f.null:
                field_output.append(style.SQL_KEYWORD('NOT NULL'))
            if f.primary_key:
                field_output.append(style.SQL_KEYWORD('PRIMARY KEY'))
            elif f.unique:
                field_output.append(style.SQL_KEYWORD('UNIQUE'))
            if tablespace and f.unique:
                # We must specify the index tablespace inline, because we
                # won't be generating a CREATE INDEX statement for this field.
                tablespace_sql = self.connection.ops.tablespace_sql(
                    tablespace, inline=True)
                if tablespace_sql:
                    field_output.append(tablespace_sql)
            if f.rel:
                ref_output, pending = self.sql_for_inline_foreign_key_references(
                    f, known_models, style)
                if pending:
                    pending_references.setdefault(f.rel.to, []).append(
                        (model, f))
                else:
                    field_output.extend(ref_output)
            table_output.append(' '.join(field_output))
        for field_constraints in opts.unique_together:
            table_output.append(style.SQL_KEYWORD('UNIQUE') + ' (%s)' %
                ", ".join(
                    [style.SQL_FIELD(qn(opts.get_field(f).column))
                     for f in field_constraints]))

        ### check which column type
        table_type = django_hana.MODEL_STORE.get(model.__name__, self.connection.settings_dict.get('DEFAULT_MODEL_STORE', 'COLUMN'))

        full_statement = [style.SQL_KEYWORD('CREATE ' + table_type + ' TABLE') + ' ' +
                          style.SQL_TABLE(qn(opts.db_table)) + ' (']
        for i, line in enumerate(table_output): # Combine and add commas.
            full_statement.append(
                '    %s%s' % (line, i < len(table_output)-1 and ',' or ''))
        full_statement.append(')')
        if opts.db_tablespace:
            tablespace_sql = self.connection.ops.tablespace_sql(
                opts.db_tablespace)
            if tablespace_sql:
                full_statement.append(tablespace_sql)
        #HANA complains with semicolon at the end
        #full_statement.append(';')
        final_output.append('\n'.join(full_statement))

        if opts.has_auto_field:
            # Add any extra SQL needed to support auto-incrementing primary
            # keys.
            auto_column = opts.auto_field.db_column or opts.auto_field.name
            autoinc_sql = self.connection.ops.autoinc_sql(opts.db_table,
                                                          auto_column)
            if autoinc_sql:
                for stmt in autoinc_sql:
                    final_output.append(stmt)

        return final_output, pending_references


        
    def sql_for_inline_foreign_key_references(self, field, known_models, style):
        """
        Return the SQL snippet defining the foreign key reference for a field.
                Foreign key not supported
        """
        return [],False
        
    def sql_destroy_model(self, model, references_to_delete, style):
        """
            Return the DROP TABLE and restraint dropping statements for a single
            model.
        """
        if not model._meta.managed or model._meta.proxy or model._meta.swapped:
            return []
        # Drop the table now
        qn = self.connection.ops.quote_name
        output = ['%s %s;' % (style.SQL_KEYWORD('DROP TABLE'),style.SQL_TABLE(qn(model._meta.db_table)))]
        
        if model._meta.has_auto_field:
            ds = self.connection.ops.drop_sequence_sql(model._meta.db_table)
            if ds:
                output.append(ds)
        return output
        
    def _create_test_db(self, verbosity, autoclobber):
        """
        Internal implementation - creates the test db tables.
        Modified because SAP HANA has schema to group related tables
        """
        suffix = self.sql_table_creation_suffix()

        test_database_name = self._get_test_db_name()

        qn = self.connection.ops.quote_name

        cursor = self.connection.cursor()
        self._prepare_for_test_db_ddl()
        try:
            cursor.execute(
                "CREATE SCHEMA %s %s" % (qn(test_database_name), suffix))
        except Exception as e:
            sys.stderr.write(
                "Got an error creating the test database: %s\n" % e)
            if not autoclobber:
                confirm = input(
                    "Type 'yes' if you would like to try deleting the test "
                    "database '%s', or 'no' to cancel: " % test_database_name)
            if autoclobber or confirm == 'yes':
                try:
                    if verbosity >= 1:
                        print("Destroying old test database '%s'..."
                              % self.connection.alias)
                    cursor.execute(
                        "DROP SCHEMA %s CASCADE" % qn(test_database_name))
                    cursor.execute(
                        "CREATE SCHEMA %s %s" % (qn(test_database_name),
                                                   suffix))
                except Exception as e:
                    sys.stderr.write(
                        "Got an error recreating the test database: %s\n" % e)
                    sys.exit(2)
            else:
                print("Tests cancelled.")
                sys.exit(1)

        return test_database_name
                
    def _destroy_test_db(self, test_database_name, verbosity):
        """
        Internal implementation - remove the test db tables.
        """
        # Remove the test database to clean up after
        # ourselves. Connect to the previous database (not the test database)
        # to do so, because it's not allowed to delete a database while being
        # connected to it.
        cursor = self.connection.cursor()
        self._prepare_for_test_db_ddl()
        # Wait to avoid "database is being accessed by other users" errors.
        time.sleep(1)
        cursor.execute("DROP SCHEMA %s CASCADE"
                       % self.connection.ops.quote_name(test_database_name))
        self.connection.close()

    def sql_indexes_for_field(self, model, f, style):
        """
        Return the CREATE INDEX SQL statements for a single model field.
        """
        from django.db.backends.util import truncate_name

        if f.db_index and not f.unique:
            qn = self.connection.ops.quote_name
            tablespace = f.db_tablespace or model._meta.db_tablespace
            if tablespace:
                tablespace_sql = self.connection.ops.tablespace_sql(tablespace)
                if tablespace_sql:
                    tablespace_sql = ' ' + tablespace_sql
            else:
                tablespace_sql = ''
            i_name = '%s_%s' % (model._meta.db_table, self._digest(f.column))
            #HANA complains with semicolon at the end
            output = [style.SQL_KEYWORD('CREATE INDEX') + ' ' +
                style.SQL_TABLE(qn(truncate_name(
                    i_name, self.connection.ops.max_name_length()))) + ' ' +
                style.SQL_KEYWORD('ON') + ' ' +
                style.SQL_TABLE(qn(model._meta.db_table)) + ' ' +
                "(%s)" % style.SQL_FIELD(qn(f.column)) +
                "%s" % tablespace_sql]
        else:
            output = []
        return output


