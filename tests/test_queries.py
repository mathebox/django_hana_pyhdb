import unittest

from django.db import connection, models
from mock import call

from .mock_db import mock_hana, patch_db_execute
from .models import TestModel


class DatabaseConnectionMixin(object):
    @mock_hana
    @patch_db_execute
    def setUp(self, mock_execute):
        connection.ensure_connection()


class TestSetup(DatabaseConnectionMixin, unittest.TestCase):
    @mock_hana
    @patch_db_execute
    def setUp(self, mock_execute):
        connection.ensure_connection()

    @mock_hana
    @patch_db_execute
    def test_create_table(self, mock_execute):
        expected_statements = [
            call(
                'CREATE COLUMN TABLE "TEST_DHP_TESTMODEL" '
                '("ID" integer NOT NULL PRIMARY KEY, "FIELD" nvarchar(100) NOT NULL)',
                None
            ),
            call(
                'CREATE SEQUENCE "TEST_DHP_TESTMODEL_ID_SEQ" '
                'RESET BY SELECT IFNULL(MAX("ID"),0) + 1 FROM "TEST_DHP_TESTMODEL"',
                []
            ),
        ]

        with connection.schema_editor() as editor:
            editor.create_model(TestModel)
        self.assertEqual(mock_execute.call_args_list, expected_statements)

    @mock_hana
    @patch_db_execute
    def test_add_column_default_value(self, mock_execute):
        expected_statements = [
            call(
                'ALTER TABLE "TEST_DHP_TESTMODEL" '
                'ADD ("NEW_CHAR_FIELD" nvarchar(50) DEFAULT "default_value" NOT NULL)',
                []
            ),
            call(
                'ALTER TABLE "TEST_DHP_TESTMODEL" '
                'ALTER ("NEW_CHAR_FIELD" nvarchar(50) DEFAULT "default_value" NOT NULL)',
                []
            ),
        ]

        with connection.schema_editor() as editor:
            field = models.CharField(max_length=50, default='default_value')
            field.set_attributes_from_name('new_char_field')
            editor.add_field(TestModel, field)
        self.assertEqual(mock_execute.call_args_list, expected_statements)


class TestSelection(DatabaseConnectionMixin, unittest.TestCase):
    @mock_hana
    @patch_db_execute
    def test_select_model(self, mock_execute):
        expected_statements = [
            call('SELECT "TEST_DHP_TESTMODEL"."ID", "TEST_DHP_TESTMODEL"."FIELD" FROM "TEST_DHP_TESTMODEL"', ()),
        ]

        list(TestModel.objects.all())  # trigger database query with list()
        self.assertEqual(mock_execute.call_args_list, expected_statements)
