import unittest

from django.db import connection, models
from mock import call

from .mock_db import mock_hana, patch_db_execute, patch_db_executemany, patch_db_fetchmany, patch_db_fetchone
from .models import TestModel


class DatabaseConnectionMixin(object):
    @mock_hana
    @patch_db_execute
    @patch_db_fetchone
    def setUp(self, mock_fetchone, mock_execute):
        connection.ensure_connection()


class TestSetup(DatabaseConnectionMixin, unittest.TestCase):
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
                'ADD ("NEW_CHAR_FIELD" nvarchar(50) NOT NULL)',
                []
            ),
        ]

        with connection.schema_editor() as editor:
            field = models.CharField(max_length=50, default='default_value')
            field.set_attributes_from_name('new_char_field')
            editor.add_field(TestModel, field)
        self.assertEqual(mock_execute.call_args_list, expected_statements)


class TestCreation(DatabaseConnectionMixin, unittest.TestCase):
    @mock_hana
    @patch_db_execute
    @patch_db_fetchone
    def test_insert_object(self, mock_fetchone, mock_execute):
        expected_statements = [
            call(
                'INSERT INTO "TEST_DHP_TESTMODEL" (id,"FIELD") '
                'VALUES (test_dhp_testmodel_id_seq.nextval, ?)',
                ['foobar']
            ),
            call('select test_dhp_testmodel_id_seq.currval from dummy', ()),
        ]
        mock_fetchone.side_effect = [[1]]

        TestModel.objects.create(field='foobar')
        self.assertEqual(mock_execute.call_args_list, expected_statements)

    @mock_hana
    @patch_db_executemany
    @patch_db_fetchone
    def test_insert_objects(self, mock_fetchone, mock_execute):
        expected_statements = [
            call(
                'INSERT INTO "TEST_DHP_TESTMODEL" (id,"FIELD") '
                'VALUES (test_dhp_testmodel_id_seq.nextval, ?)',
                (['foobar'], ['barbaz'])
            ),
        ]
        mock_fetchone.side_effect = [[1]]

        TestModel.objects.bulk_create([
            TestModel(field='foobar'),
            TestModel(field='barbaz'),
        ])

        self.assertSequenceEqual(mock_execute.call_args_list, expected_statements)


class TestSelection(DatabaseConnectionMixin, unittest.TestCase):
    @mock_hana
    @patch_db_execute
    @patch_db_fetchmany
    def test_select_model(self, mock_fetchmany, mock_execute):
        expected_statements = [
            call('SELECT "TEST_DHP_TESTMODEL"."ID", "TEST_DHP_TESTMODEL"."FIELD" FROM "TEST_DHP_TESTMODEL"', ()),
        ]
        mock_fetchmany.side_effect = [[]]  # return empty list

        list(TestModel.objects.all())  # trigger database query with list()
        self.assertEqual(mock_execute.call_args_list, expected_statements)


class TestAggregation(DatabaseConnectionMixin, unittest.TestCase):
    @mock_hana
    @patch_db_execute
    @patch_db_fetchone
    def test_aggregate(self, mock_fetchone, mock_execute):
        expected_statements = [
            call('SELECT COUNT("TEST_DHP_TESTMODEL"."FIELD") AS "FIELD__COUNT" FROM "TEST_DHP_TESTMODEL"', ())
        ]
        field_count = 1
        mock_fetchone.side_effect = [[field_count]]

        data = TestModel.objects.all().aggregate(models.Count('field'))

        self.assertIn('field__count', data)
        self.assertEqual(data['field__count'], field_count)
        self.assertEqual(mock_execute.call_args_list, expected_statements)

    @mock_hana
    @patch_db_execute
    @patch_db_fetchmany
    def test_annotate(self, mock_fetchmany, mock_execute):
        expected_statements = [
            call(
                'SELECT "TEST_DHP_TESTMODEL"."ID", "TEST_DHP_TESTMODEL"."FIELD", '
                'COUNT("TEST_DHP_TESTMODEL"."FIELD") AS "NUM_FIELDS" '
                'FROM "TEST_DHP_TESTMODEL" '
                'GROUP BY "TEST_DHP_TESTMODEL"."ID", "TEST_DHP_TESTMODEL"."FIELD"',
                ()
            ),
        ]
        mock_fetchmany.side_effect = [[]]  # return empty list

        qs = TestModel.objects.annotate(num_fields=models.Count('field'))
        list(qs)  # trigger database query with list()
        self.assertEqual(mock_execute.call_args_list, expected_statements)

    @mock_hana
    @patch_db_execute
    @patch_db_fetchone
    def test_annotate_aggreate(self, mock_fetchone, mock_execute):
        expected_statements = [
            call(
                'SELECT SUM("NUM_FIELDS") '
                'FROM (SELECT "TEST_DHP_TESTMODEL"."ID" AS Col1, COUNT("TEST_DHP_TESTMODEL"."FIELD") AS "NUM_FIELDS" '
                'FROM "TEST_DHP_TESTMODEL" '
                'GROUP BY "TEST_DHP_TESTMODEL"."ID") subquery',
                ()
            ),
        ]
        num_fields = 1
        mock_fetchone.side_effect = [[num_fields]]

        data = TestModel.objects.annotate(num_fields=models.Count('field')).aggregate(models.Sum('num_fields'))

        self.assertIn('num_fields__sum', data)
        self.assertEqual(data['num_fields__sum'], num_fields)
        self.assertEqual(mock_execute.call_args_list, expected_statements)
