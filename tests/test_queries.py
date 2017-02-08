import unittest

import mock
from django.core.management.color import no_style
from django.db import connection, models
from django.db.backends.base.schema import BaseDatabaseSchemaEditor

from .models import TestModel


class DatabaseSQLMixin(object):
    def remove_whitespace(self, statement):
        return ' '.join(str(statement).replace('\n', '').split())

    def assertQueryEqual(self, query1, query2):
        self.assertEqual(self.remove_whitespace(query1), self.remove_whitespace(query2))


class TestCreation(DatabaseSQLMixin, unittest.TestCase):
    style = no_style()

    @mock.patch.object(BaseDatabaseSchemaEditor, 'execute')
    def test_create_table(self, mock_execute):
        expected_statements = [
            (((
                'CREATE COLUMN TABLE "TEST_DHP_TESTMODEL" ('
                '"ID" integer NOT NULL PRIMARY KEY, "FIELD" nvarchar(100) NOT NULL)'
            ), None),),
            (((
                'CREATE SEQUENCE "TEST_DHP_TESTMODEL_ID_SEQ" '
                'RESET BY SELECT IFNULL(MAX("ID"),0) + 1 FROM "TEST_DHP_TESTMODEL"'
            ),),),
        ]

        with connection.schema_editor() as editor:
            editor.create_model(TestModel)
        self.assertEqual(mock_execute.call_args_list, expected_statements)

    @mock.patch.object(BaseDatabaseSchemaEditor, 'execute')
    def test_add_column_default_value(self, mock_execute):
        expected_statements = [
            (((
                'ALTER TABLE "TEST_DHP_TESTMODEL" '
                'ADD ("NEW_CHAR_FIELD" nvarchar(50) DEFAULT "default_value" NOT NULL)'
            ), []),),
            (((
                'ALTER TABLE "TEST_DHP_TESTMODEL" '
                'ALTER ("NEW_CHAR_FIELD" nvarchar(50) DEFAULT "default_value" NOT NULL)'
            ),),),
        ]

        with connection.schema_editor() as editor:
            field = models.CharField(max_length=50, default='default_value')
            field.set_attributes_from_name('new_char_field')
            editor.add_field(TestModel, field)
        self.assertEqual(mock_execute.call_args_list, expected_statements)


class TestSelection(DatabaseSQLMixin, unittest.TestCase):

    def test_select_model(self):
        expected = (
            'SELECT "TEST_DHP_TESTMODEL"."ID", "TEST_DHP_TESTMODEL"."FIELD" '
            'FROM "TEST_DHP_TESTMODEL"'
        )

        qs = TestModel.objects.all()
        self.assertQueryEqual(qs.query, expected)
